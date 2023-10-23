using TransactionManagerTransactionManagerProto;
using TransactionManagerLeaseManagerServiceProto;
using ClientTransactionManagerProto;
using Google.Protobuf.WellKnownTypes;
using System.Transactions;
using Google.Protobuf.Collections;

namespace TKVTransactionManager.Services
{
    using LeaseManagers =
        Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient>;

    public struct TransactionState
    {
        public List<string> Leases { get; set; }
        public TransactionRequest Request { get; set; }
    }

    public class ServerService
    {
        // Config file variables
        private readonly string _processId;
        private readonly List<List<bool>> _tmsStatePerSlot; // all TMs states per slot

        private readonly List<List<string>>
            _tmsSuspectedPerSlot; // processes that this TM suspects to be crashed PER slot

        private readonly Dictionary<string, Gossip.GossipClient> _transactionManagers;
        private readonly LeaseManagers _leaseManagers;
        private readonly int _processIndex;

        // Paxos variables
        private bool _isCrashed;
        private int _currentSlot; // The number of experienced slots (process may be frozen and not experience all slots)

        // Replication variables
        private Dictionary<string, DADInt> _transactionManagerDadInts;

        private List<Lease> _leasesHeld;

        private bool _allLeases;

        private List<DADInt> _dadIntsRead;

        private List<TransactionState> _transactionsState;

        private List<DADInt> _writeLog;

        public ServerService(
            string processId,
            Dictionary<string, Gossip.GossipClient> transactionManagers,
            LeaseManagers leaseManagers,
            List<List<bool>> tmsStatePerSlot,
            List<List<string>> tmsSuspectedPerSlot,
            int processIndex
        )
        {
            _processId = processId;
            _transactionManagers = transactionManagers;
            _leaseManagers = leaseManagers;
            _tmsStatePerSlot = tmsStatePerSlot;
            _tmsSuspectedPerSlot = tmsSuspectedPerSlot;
            _processIndex = processIndex;

            _isCrashed = false;
            _currentSlot = 0;
            _transactionManagerDadInts = new Dictionary<string, DADInt>();
            _leasesHeld = new List<Lease>();

            _allLeases = false;
            _dadIntsRead = new List<DADInt>();
            _transactionsState = new List<TransactionState>();
            _writeLog = new List<DADInt>();
        }

        public void PrepareSlot()
        {
            Monitor.Enter(this);

            // End of slots
            if (_currentSlot >= _tmsStatePerSlot.Count)
            {
                Console.WriteLine("Slot duration ended but no more slots to process.");
                return;
            }

            Console.WriteLine("========== Preparing new slot =========================");

            // get process state
            _isCrashed = _tmsStatePerSlot[_currentSlot][_processIndex];

            Console.WriteLine($"State: Process is now {(_isCrashed ? "crashed" : "normal")} for slot {_currentSlot}\n");

            // Global slot counter
            _currentSlot++;

            // If process is crashed, don't do anything
            if (_isCrashed)
            {
                //Console.WriteLine("Ending preparation -----------------------");
                Monitor.Exit(this);
                return;
            }

            Monitor.PulseAll(this);

            Console.WriteLine("Updating Transaction Log");
            UpdateTransactionLogStatus();

            Console.WriteLine("Requesting a Paxos Update");
            AskForLeaseManagersStatus();


            Monitor.Exit(this);
        }
 
        public StatusResponse Status(StatusRequest statusRequest)
        {
            if (_isCrashed) { Monitor.Wait(this); }
            return new StatusResponse { Status = true };
        }

        public TransactionResponse TxSubmit(TransactionRequest transactionRequest)
        {
            if (_isCrashed) { Monitor.Wait(this); }

            _dadIntsRead = new List<DADInt>();

            var transactionState = new TransactionState { Leases = new List<string>(), Request = transactionRequest };

            Console.WriteLine($"Received transaction request from: {transactionRequest.Id}");

            var leasesRequired = transactionRequest.Reads.ToList();
            leasesRequired.AddRange(transactionRequest.Writes.Select(dadint => dadint.Key));

            // check if has all leases
            transactionState.Leases = leasesRequired
                .Where(lease => !_leasesHeld.Any(leaseHeld => leaseHeld.Permissions.Contains(lease)))
                .ToList();
            _transactionsState.Add(transactionState);


            // if TM doesn't have all leases it must request them
            if (transactionState.Leases.Count > 0)
            {
                Console.WriteLine($"Requesting leases...");
                var lease = new Lease { Id = _processId };
                lease.Permissions.AddRange(transactionState.Leases);
                var leaseRequest = new LeaseRequest { Slot = _currentSlot, Lease = lease };

                var tasks = new List<Task>();
                foreach (var host in _leaseManagers)
                {
                    var t = Task.Run(() =>
                    {
                        try
                        {
                            _leaseManagers[host.Key].Lease(leaseRequest);
                        }
                        catch (Grpc.Core.RpcException e)
                        {
                            Console.WriteLine(e.Status);
                        }
                        
                        return Task.CompletedTask;
                    });
                    tasks.Add(t);
                }
            }

            Console.WriteLine($"Finished processing transaction request...");
            var transactionResponse = new TransactionResponse();
            transactionResponse.Response.AddRange(_dadIntsRead);
            return transactionResponse;
        }

        public void AskForLeaseManagersStatus()
        {
            Console.WriteLine("Requesting status update from lease managers...");
            var statusUpdateResponse = new StatusUpdateResponse();
            var tasks = new List<Task>();
            foreach (var host in _leaseManagers)
            {
                var t = Task.Run(() =>
                {
                    statusUpdateResponse = _leaseManagers[host.Key].StatusUpdate(new Empty());
                    return Task.CompletedTask;
                });
                tasks.Add(t);
            }

            // they should all be the same, so we can just wait for one
            for (var i = 0; i < _leaseManagers.Count / 2 + 1; i++)
            {
                tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
            }

            Monitor.Enter(this);

            //Console.WriteLine($"Received status update from lease managers");

            Console.WriteLine($"    Got ({statusUpdateResponse.Leases.Count}) Leases.");

            // we are going to check for conflict leases. a lease is in conflict if a TM holds a lease for a key that was given as permission to another TM in a later Lease.
            // if the lease is in conflict, this TM should release the Lease it holds. 

            /* ! Description of the algorithm and TODO list:

                 Look through all transactions that we want to execute and check if we have all the leases required to execute them.
                 If not, too bad! We'll store them somewhere and wait for the next slot to try again.
                 If the lease manager assign two or more leases to the same key in one slot, we'll have to wait for the other lease to execute before we execute ours. (We'll be warned by that TM)
                 If we're not warned by the other TM within that slot, we can assume that the other TM is crashed and we can execute our transaction in the next slot.
                 If we're warned by the other TM that they executed, we can execute!
                 If none of the above happens, we can execute!
                 Before executing we first need to Gossip the transaction to all other TMs.
                 Then we are done! Just to do this for all transactions that we have stored.
             */

            // TODO: Store the transactions somewhere until the next turn if we can't execute them (is this done automatically by the foreach loop below?).
            // TODO: Before we execute and gossip we need to make sure all conditions are okay!

            CheckLeaseConflicts(statusUpdateResponse);

            // foreach transaction state, check if we have all the leases required to execute it
            foreach (var transactionState in _transactionsState)
            {
                // if we have all the leases required to execute it, we can execute it
                if (transactionState.Leases.Count == 0)
                {
                    // get the list of leases required to execute the transaction
                    var leasesRequired = transactionState.Request.Reads.ToList();
                    leasesRequired.AddRange(transactionState.Request.Writes.Select(dadint => dadint.Key));

                    var leasesToWaitOn = new List<Lease>();

                    // TODO: not sure if iterating properly 🤔
                    for (var i = 1; i < statusUpdateResponse.Leases.Count; i++)
                    {
                        var lease = statusUpdateResponse.Leases[i];
                        var leaseToCheck = statusUpdateResponse.Leases[i - 1];

                        // if any of the permissions match and the id is different, we need to wait on that TM
                        if (leasesRequired.Any(leaseRequired => lease.Permissions.Contains(leaseRequired)) &&
                            lease.Id != leaseToCheck.Id)
                        {
                            leasesToWaitOn.Add(leaseToCheck);
                        }
                    }
                    
                    // ask all TM that we need to wait on to tell us when they execute the lease that we want
                    var tasks2 = new List<Task>();
                    foreach (var leaseToWaitOn in leasesToWaitOn)
                    {
                        var t = Task.Run(() =>
                        {
                            try
                            {
                                var sameSlotLeaseExecutionRequest = new SameSlotLeaseExecutionRequest
                                    { Lease = leaseToWaitOn };
                                _transactionManagers[leaseToWaitOn.Id]
                                    .SameSlotLeaseExecution(sameSlotLeaseExecutionRequest);
                            }
                            catch (Grpc.Core.RpcException e)
                            {
                                Console.WriteLine(e.Status);
                            }
                            return Task.CompletedTask;
                        });
                        tasks2.Add(t);
                    }

                    // wait for all of them to reply
                    Task.WaitAll(tasks2.ToArray());

                    GossipTransaction(transactionState);
                    ExecuteTransaction(transactionState);
                }
            }

            Monitor.Exit(this);
        }

        /* TODO: check if this function does what it's supposed to do.
         Firstly, we need to check if the lease was assigned to us. If it was, we add it to our list of leases held.
         Secondly, we need to check if any of the permissions of the lease was assigned to somebody else. If it was, we remove it from our list of leases held.
         Thirdly, it does not do anything if two leases with the same permissions are assigned in the same slot. We need to check for that and wait for the other TM to execute it.
         */
        private void CheckLeaseConflicts(StatusUpdateResponse statusUpdateResponse)
        {
            var leasesHeldBeforeRunning = _leasesHeld;
            foreach (var lease in statusUpdateResponse.Leases)
            {
                // if the lease was assigned to us, we add it to our list of leases held
                if (lease.Id == _processId)
                {
                    _leasesHeld.Add(lease);
                }
                else
                {
                    // if the lease was in leasesHeldBeforeRunning we remove it from our list of leases held
                    // this guarantees that we do not mess with leases assigned in the same slot
                    if (leasesHeldBeforeRunning.Contains(lease))
                    {
                        _leasesHeld.RemoveAll(leaseHeld => leaseHeld.Permissions.Contains(lease.Permissions[0]));
                    }
                }
            }
        }

        public void ExecuteTransaction(TransactionState transactionState)
        {
            Console.WriteLine($"Finally executing transaction...");
            foreach (var dadintKey in transactionState.Request.Reads)
            {
                if (_transactionManagerDadInts.TryGetValue(dadintKey, out var dadint))
                    _dadIntsRead.Add(dadint);
                else
                {
                    // TODO: else what?
                }
                {
                    Console.WriteLine("Requested read on non-existing DADINT.");
                }
            }

            WriteTransactions(transactionState.Request.Writes);
            _transactionsState.Remove(transactionState);
            Monitor.PulseAll(this);
        }

        private void WriteTransactions(RepeatedField<DADInt> writes)
        {
            foreach (var dadint in writes)
            {
                if (_transactionManagerDadInts.TryGetValue(dadint.Key, out var j))
                {
                    try
                    {
                        j.Value = dadint.Value;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }
                else
                {
                    _transactionManagerDadInts.Add(dadint.Key, new DADInt { Key = dadint.Key, Value = dadint.Value });
                }

                _writeLog.Add(dadint);
            }
        }

        public void GossipTransaction(TransactionState transaction)
        {
            /* This is just a matter of sending the write part of a transaction to all other TMs.
                 Once they have replied saying that a majority of them executed the transaction, we can execute it.
                 Do we need two phases here? We have to think about it.
              */

            Console.WriteLine($"    Gossiping transaction...");
            var tasks = new List<Task>();
            var responses = new List<GossipResponse>();

            // via this special pipeline, we only send the write set of the transaction
            var dadint = transaction.Request.Writes;
            foreach (var host in _transactionManagers)
            {
                var t = Task.Run(() =>
                {
                    try
                    {
                        GossipRequest gossipRequest = new();
                        gossipRequest.Writes.AddRange(dadint);
                        var gossipResponse = _transactionManagers[host.Key].Gossip(gossipRequest); // should we send to all TM or ignore the crashed ones?
                        responses.Add(gossipResponse);
                    }
                    catch (Grpc.Core.RpcException e)
                    {
                        Console.WriteLine(e.Status);
                    }
                    return Task.CompletedTask;
                });
                tasks.Add(t);
            }

            for (var i = 0; i < _transactionManagers.Count / 2 + 1; i++)
            {
                tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
            }
            Console.WriteLine($"Gossiping transaction... Done!");
        }

        public GossipResponse ReceiveGossip(GossipRequest request)
        {
            /* When a gossip request is received we first reply to the TM saying that we have received it and that we will execute it.
                Do we need two phases here? We have to think about it.
             */

            // TODO: Think of possible failure cases.

            WriteTransactions(request.Writes);

            return new GossipResponse { Ok = true };
        }

        public void UpdateTransactionLogStatus()
        {
            /* When we begin a new slot, we ask a majority of process for their written transaction logs, one of them is the latest one!
                 The latest one is the biggest one, so we just need to compare the sizes of the logs.
                 If it's different from ours, we need to update our log to the latest one!
                 We also need to give up all the leases that we hold.
                 We do this by deleting everything we have and rebuilding it from the latest one.
                 This is very slow, but it's the simplest way to do it and I frankly don't care anymore.
             */

            // ask for logs from all other TMs and wait for a majority of them to reply
            var tasks = new List<Task>();
            var responses = new List<UpdateResponse>();

            foreach (var host in _transactionManagers)
            {
                var t = Task.Run(() =>
                {
                    try
                    {
                        var updateResponse = _transactionManagers[host.Key].Update(new UpdateRequest());
                        responses.Add(updateResponse);
                    }
                    catch (Grpc.Core.RpcException e)
                    {
                        Console.WriteLine(e.Status);
                    }
                    return Task.CompletedTask;
                });
                tasks.Add(t);
            }
            
            // wait for a majority of them to reply
            for (var i = 0; i < _transactionManagers.Count / 2 + 1; i++)
            {
                tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
            }

            // TODO: we need to change this to get the biggest log that appears most often (there can be multiple logs with the same size)

            // compare the sizes of the logs and get the biggest one
            var biggestLog = responses.Aggregate((i1, i2) => i1.Writes.Count > i2.Writes.Count ? i1 : i2);

            // if the biggest one is different from ours and if they logs is not empty, we need to update our log to the latest one
            if (biggestLog.Writes.Count != _writeLog.Count && biggestLog.Writes.Count != 0)
            {
                UpdateLocalLog(biggestLog);
            }
        }

        private void UpdateLocalLog(UpdateResponse updateResponse)
        {
            // give up all the leases that we hold
            _leasesHeld = new List<Lease>();

            // TODO: completely reset the state of the TM (is this enough?)
            _transactionManagerDadInts = new Dictionary<string, DADInt>();
            _dadIntsRead = new List<DADInt>();
            _transactionsState = new List<TransactionState>();
            
            // TODO: is this enough to recover the state of the TM?
            // rewrite all the transactions that in the _writeLog
            var updateResponseWriteLog = updateResponse.Writes;
            WriteTransactions(updateResponseWriteLog);
        }

        public UpdateResponse ReplyWithUpdate(UpdateRequest request)
        {
            /* When we receive an update request, we need to reply back with our write log. */
            var updateResponse = new UpdateResponse();
            updateResponse.Writes.AddRange(_writeLog);
            return updateResponse;
        }

        // this method is for when a TM asks us to tell them when we execute a transaction that they have a lease for.
        public SameSlotLeaseExecutionResponse SameSlotLeaseExecution(SameSlotLeaseExecutionRequest request)
        {
            // TODO: not sure if locks are needed here
            // go to sleep
            Monitor.Enter(this);
            // wait for the lease that is in the request to not be in the list of leases held
            while (_leasesHeld.Any(leaseHeld => leaseHeld.Permissions.Contains(request.Lease.Permissions[0])))
            {
                // thread will be woken up by the monitorpulseall in the execute transaction method
            }
            Monitor.Exit(this);
            return new SameSlotLeaseExecutionResponse { Lease = request.Lease };
        }
    }
}