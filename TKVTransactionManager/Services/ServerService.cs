using TransactionManagerTransactionManagerProto;
using TransactionManagerLeaseManagerServiceProto;
using ClientTransactionManagerProto;
using Google.Protobuf.WellKnownTypes;
using System.Transactions;

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

        private readonly Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> _transactionManagers;
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

        public ServerService(
            string processId,
            Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers,
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
            _transactionManagerDadInts = new();
            _leasesHeld = new List<Lease>();

            _allLeases = false;
            _dadIntsRead = new List<DADInt>();
            _transactionsState = new List<TransactionState>();
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

            Console.WriteLine($"Preparing slot... ------------------------------------------");

            // get process state
            _isCrashed = _tmsStatePerSlot[_currentSlot][_processIndex];

            Console.WriteLine($"Process is now {(_isCrashed ? "crashed" : "normal")}");

            // Global slot counter
            _currentSlot++;

            // If process is crashed, don't do anything
            if (_isCrashed)
            {
                Console.WriteLine("Ending preparation -----------------------");
                Monitor.Exit(this);
                return;
            }

            Monitor.PulseAll(this);

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

            var leasesRequired = new List<string>();
            _dadIntsRead = new List<DADInt>();

            TransactionState transactionState = new TransactionState { Leases = new(), Request = transactionRequest };

            Console.WriteLine($"Received transaction request FROM: {transactionRequest.Id}");

            foreach (var dadintKey in transactionRequest.Reads)
            {
                //Console.WriteLine($"     DADINT2READ: {dadintKey}");
                // add to leasesRequired
                leasesRequired.Add(dadintKey);
            }

            foreach (var dadint in transactionRequest.Writes)
            {
                //Console.WriteLine($"     DADINT2RWRITE: {dadint.Key}:{dadint.Value}");
                leasesRequired.Add(dadint.Key);
            }

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
            foreach (var lease in statusUpdateResponse.Leases)
            {
                // Check if the lease is held by another process (TM)
                if (lease.Id == _processId)
                {
                    //Console.WriteLine("     Adding new lease...");
                    _leasesHeld.Add(lease);

                    foreach (TransactionState tsState in _transactionsState.Where(ts => ts.Leases.Count > 0))
                    {
                        tsState.Leases.RemoveAll(leasePerm => lease.Permissions.Contains(leasePerm));
                    }
                    continue;
                }

                // Iterate through leases held by the current process
                foreach (var heldLease in _leasesHeld.ToList().Where(heldLease => lease.Permissions.Intersect(heldLease.Permissions).Any()))
                {
                    // Remove conflicting lease
                    _leasesHeld.Remove(heldLease);

                    // Exit inner loop since a conflicting lease was removed
                    break;
                }
            }

            /* TODO: Look through all transactions that we want to execute and check if we have all the leases required to execute them.
                 If not, too bad! We'll store them somewhere and wait for the next slot to try again.
                 If the lease manager assign two or more leases to the same key in one slot, we'll have to wait for the other lease to execute before we execute ours. (We'll be warned by that TM)
                 If we're not warned by the other TM within that slot, we can assume that the other TM is crashed and we can execute our transaction in the next slot.
                 If we're warned by the other TM that they executed, we can execute!
                 If none of the above happens, we can execute!
                 Before executing we first need to Gossip the transaction to all other TMs.
                 Then we are done! Just to do this for all transactions that we have stored.
             */

            Monitor.Exit(this);
        }

        public void ExecuteTransaction()
        {
            /* TODO: Executing a transaction means doing the executing stuff and adding to our log of all transactions executed so far!
             */
        }

        public void GossipTransaction()
        {
            /* TODO: This is just a matter of sending a transaction to all other TMs.
                 We'll have to wait for a majority of TMs to reply back saying that they have received it.
                 If we don't receive a majority, we'll have to wait for the next slot to try again.
                 Once they have replied saying that they received the transaction, we'll tell them to execute it.
              */
        }

        public void receiveGossip()
        {
            /* TODO: When a gossip request is received we first reply to the TM saying that we have received it!
                 After they reply back saying we can execute the transaction and add it to our log.
                 If it any point they don't reply back, we assume that they are crashed and do nothing.
             */
        }

        public void updateTransactionLogStatus()
        {
            /* TODO: When we begin a new slot, we ask a majority of process for their logs, one of them is the latest one!
                 The latest one is the biggest one, so we just need to compare the sizes of the logs.
                 If it's different from ours, we need to update our log to the latest one!
                 We do this by deleting everything we have and rebuilding it from the latest one.
                 This is very slow, but it's the simplest way to do it and I frankly don't care anymore.
             */
        }
    }
}