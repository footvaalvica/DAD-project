using TransactionManagerTransactionManagerProto;
using TransactionManagerLeaseManagerServiceProto;
using ClientTransactionManagerProto;
using Google.Protobuf.WellKnownTypes;
using System.Transactions;
using Google.Protobuf.Collections;
using System.Diagnostics;
using ExtensionMethods;
using System.Threading;
using System.Threading.Tasks;

namespace ExtensionMethods
{
    public static class ListExtensions
    {
        public static T MostCommon<T>(this IEnumerable<T> list)
        {
            return list.GroupBy(i=>i).OrderByDescending(grp=>grp.Count()).Select(grp=>grp.Key).First();
        }
    }
}

namespace TKVTransactionManager.Services
{
    using LeaseManagers =
        Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient>;


    public struct TransactionState
    {
        public List<(string, bool)> Permissions { get; set; }
        public TransactionRequest Request { get; set; }
        public int Index { get; set; }
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

        private List<string> _processBook;

        private List<string> _reachableTMs = new();

        private CancellationTokenSource _cancelPreviousActiveTasks;

        public ServerService(
            string processId,
            Dictionary<string, Gossip.GossipClient> transactionManagers,
            LeaseManagers leaseManagers,
            List<List<bool>> tmsStatePerSlot,
            List<List<string>> tmsSuspectedPerSlot,
            int processIndex,
            List<string> processBook
        )
        {
            _processId = processId;
            _transactionManagers = transactionManagers;
            _leaseManagers = leaseManagers;
            _tmsStatePerSlot = tmsStatePerSlot;
            _tmsSuspectedPerSlot = tmsSuspectedPerSlot;
            _processIndex = processIndex;
            _processBook = processBook;

            _isCrashed = false;
            _currentSlot = -1; // begin at -1 so that the first slot is 0
            _transactionManagerDadInts = new Dictionary<string, DADInt>();
            _leasesHeld = new List<Lease>();
                
            _allLeases = false;
            _dadIntsRead = new List<DADInt>();
            _transactionsState = new List<TransactionState>();
            _writeLog = new List<DADInt>();

            _cancelPreviousActiveTasks = new CancellationTokenSource();
        }

        public void PrepareSlot()
        {
            Monitor.Enter(this);

            // Global slot counter
            _currentSlot++;

            // End previous slot if it's still waiting on tasks
            _cancelPreviousActiveTasks.Cancel();
            _cancelPreviousActiveTasks = new CancellationTokenSource();

            // End of slots
            if (_currentSlot >= _tmsStatePerSlot.Count)
            {
                Console.WriteLine("Slot duration ended but no more slots to process.");
                return;
            }

            Console.WriteLine("================ Preparing new slot =========================");

            // get process state
            _isCrashed = _tmsStatePerSlot[_currentSlot][_processIndex];

            Console.WriteLine($"State: Process is now {(_isCrashed ? "crashed" : "normal")} for slot {_currentSlot}\n");

            // If process is crashed, don't do anything
            if (_isCrashed)
            {
                Monitor.Exit(this);
                return;
            }

            // unsuspected hosts + not including self
            _reachableTMs = _transactionManagers.Where(host => host.Key != _processId && !_tmsSuspectedPerSlot[_currentSlot]
                .Contains(host.Key)).Select(host => host.Key).ToList();

            Monitor.PulseAll(this);

            try
            {
                UpdateTransactionLogStatus();
                AskForLeaseManagersStatus();
            }
            catch (MajorityInsufficiencyException e)
            {
                Console.WriteLine(e.Message);
                Monitor.Exit(this);
                return;
            }
            catch (SlotExecutionTimeoutException e)
            {
                Console.WriteLine(e.Message);
                Monitor.Exit(this);
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Monitor.Exit(this);
                return;
            }

            Monitor.Exit(this);
        }
 
        public StatusResponseTM Status(StatusRequestTM statusRequest)
        {
            Console.WriteLine("<<<<<<<<<<<<< STATUS >>>>>>>>>>>>>>>>");
            Console.WriteLine($" Status request received for TM " + _processId);
            Console.WriteLine($"    Current slot: {_currentSlot}");
            Console.WriteLine($"    State: " + (_isCrashed ? "crashed" : "normal"));
            Console.WriteLine($"    WriteLog size: {_writeLog.Count}");
            foreach (var dadint in _writeLog)
            {
                Console.WriteLine($"        {dadint.Key}: {dadint.Value}");
            }
            Console.WriteLine($"    Leases held: {_leasesHeld.Count}");
            foreach (var lease in _leasesHeld)
            {
                Console.WriteLine($"        {lease.Id}: {string.Join(", ", lease.Permissions)}");
            }
            Console.WriteLine("<<<<<<<<<<<<< STATUS >>>>>>>>>>>>>>>>");
            return new StatusResponseTM { };
        }

        public TransactionResponse TxSubmit(TransactionRequest transactionRequest)
        {
            if (_isCrashed) { Monitor.Wait(this); }

            Monitor.Enter(this);

            _dadIntsRead = new List<DADInt>();

            var transactionState = new TransactionState { Permissions = new List<(string, bool)>(), Request = transactionRequest };

            Console.WriteLine($"Received transaction request from: {transactionRequest.Id}");

            var leasesRequired = transactionRequest.Reads.ToList();
            leasesRequired.AddRange(transactionRequest.Writes.Select(dadint => dadint.Key));

            // check if has all leases
            List<string> temp = leasesRequired.Where(lease => !_leasesHeld.Any(leaseHeld => leaseHeld.Permissions.Contains(lease))).ToList();
            foreach (var lease in temp) { transactionState.Permissions.Add((lease, false)); }

            _transactionsState.Add(transactionState);
            
            // if TM doesn't have all leases, it must request them
            if (transactionState.Permissions.Where(perm => !perm.Item2).Count() > 0)
            {
                var lease = new Lease { Id = _processId };
                lease.Permissions.AddRange(transactionState.Permissions.Where(perm => !perm.Item2).Select(perm => perm.Item1)); // get perms not yet obtained
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

            else
            {
                try
                {
                    TwoPhaseCommit(transactionState);
                    _transactionsState.Remove(transactionState);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    // abort transaction
                    _dadIntsRead.Add(new DADInt { Key = "abort", Value = null });
                }
            }

            Console.WriteLine($"Finished processing transaction request...");
            TransactionResponse transactionResponse = new TransactionResponse();
            transactionResponse.Response.AddRange(_dadIntsRead);

            Monitor.Exit(this);

            return transactionResponse;
        }

        public void AskForLeaseManagersStatus()
        {
            var statusUpdateResponse = new StatusUpdateResponse();
            var tasks = new List<Task>();

            foreach (var host in _leaseManagers)
            {
                var t = Task.Run(() =>
                {
                    statusUpdateResponse = _leaseManagers[host.Key].StatusUpdate(new Empty());
                    return Task.CompletedTask;
                }, _cancelPreviousActiveTasks.Token);
                tasks.Add(t);
            }

            try
            {
                for (var i = 0; i < _leaseManagers.Count / 2 + 1; i++)
                {
                    tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Task was canceled (most likely) due to a new slot having started.");
                throw new SlotExecutionTimeoutException();
            }

            Monitor.Enter(this);

            // check for conflicting leases. a lease is in conflict if a TM holds a lease for a key that was given as permission to another TM in a later Lease.
            // if the lease is in conflict, this TM should release the Lease it holds. 

            /* ! Description of the algorithm:

                 Look through all transactions that we want to execute and check if we have all the leases required to execute them.
                 If not, too bad! We'll store them somewhere and wait for the next slot to try again.
                 If the lease manager assign two or more leases to the same key in one slot, we'll have to wait for the other lease to execute before we execute ours. (We'll be warned by that TM)
                 If we're not warned by the other TM within that slot, we can assume that the other TM is crashed and we can execute our transaction in the next slot.
                 If we're warned by the other TM that they executed, we can execute!
                 If none of the above happens, we can execute!
                 Before executing we first need to Gossip the transaction to all other TMs.
                 Then we are done! Just to do this for all transactions that we have stored.
             */

            CheckLeaseConflicts(statusUpdateResponse);

            var transactionStatesToRemove = new List<TransactionState>();
            // foreach transaction state, check if we have all the leases required to execute it
            foreach (var transactionState in _transactionsState)
            {
                // if TM has all the leases required to execute it, do so
                if (transactionState.Permissions.Where(perm => !perm.Item2).Count() == 0)
                {
                    // check for conflicting leases given on same slot

                    // get the list of permissions required to execute the transaction
                    var permissionsRequired = transactionState.Request.Reads.ToList();
                    permissionsRequired.AddRange(transactionState.Request.Writes.Select(dadint => dadint.Key));

                    var leasesToWaitOn = new List<Lease>();
                    for (var i = 0; i < statusUpdateResponse.Leases.Count; i++)
                    {
                        // if lease ain't for this tm or not related to this transaction, continue
                        if (statusUpdateResponse.Leases[i].Id != _processId || 
                            statusUpdateResponse.Leases[i].Permissions.Where(perm => permissionsRequired.Contains(perm)).ToList().Count == 0)
                            continue;

                        for (var j = i-1; j >= 0; j--)
                        {
                            if (statusUpdateResponse.Leases[j].Id == _processId) continue;

                            // if any of the permissions match and the id is different, we need to wait on that TM
                            // BUT only if that TM is not crashed, cause otherwise it won't ever respond
                            if (permissionsRequired.Any(permRequired => statusUpdateResponse.Leases[j].Permissions.Contains(permRequired)) &&
                                statusUpdateResponse.Leases[j].Id != _processId)
                            {
                                leasesToWaitOn.Add(statusUpdateResponse.Leases[j]); // lease instead of ToCheck cause other TM needs to know which of HIS to release
                            }
                        }
                    }

                    // if new conflicting lease is assigned to currently crashed TM
                    // if so, it is invalidated by lease of this TM
                    // if it's been given to a suspected TM, leave transaction for next slot to avoid conflicts
                    
                    // ask all TM that we need to wait on to tell us when they execute the lease that we want
                    var tasks2 = new List<Task>();
                    try
                    {
                        foreach (var leaseToWaitOn in leasesToWaitOn)
                        {
                            // if it's been given to a suspected TM, leave transaction for next slot to avoid conflicts
                            if (_tmsSuspectedPerSlot[_currentSlot].Contains(leaseToWaitOn.Id))
                            {
                                // 1: ask majority opinion on whether the TM is crashed or not
                                if (SuspicionConsensus(leaseToWaitOn.Id))
                                {
                                    // 2: if majority says it's crashed, execute the transaction
                                    // 3: save information about tm being crashed for this slot? --> given teacher's answer, prolly not
                                    continue;
                                }
                                else
                                {
                                    // skip this transaction for this slot
                                    break;
                                }
                            };

                            var t = Task.Run(() =>
                            {
                                try
                                {
                                    var sameSlotLeaseExecutionRequest = new SameSlotLeaseExecutionRequest { Lease = leaseToWaitOn };
                                    _transactionManagers[leaseToWaitOn.Id].SameSlotLeaseExecution(sameSlotLeaseExecutionRequest);
                                }
                                catch (Grpc.Core.RpcException e)
                                {
                                    Console.WriteLine(e.Status);
                                }
                                return Task.CompletedTask;
                            }, _cancelPreviousActiveTasks.Token);
                            tasks2.Add(t);
                        }
                    }
                    catch (MajorityInsufficiencyException e)
                    {
                        Console.WriteLine(e.Message);
                    }
                    try
                    {
                        Task.WaitAll(tasks2.ToArray());

                        // Either we spread and execute, or we don't at all
                        TwoPhaseCommit(transactionState);
                        transactionStatesToRemove.Add(transactionState);
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("Task was canceled (most likely) due to a new slot having started.");
                        throw new SlotExecutionTimeoutException();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
            }

            // we need to remove them outside of the loop, otherwise we get an error
            transactionStatesToRemove.ForEach(transactionState => _transactionsState.Remove(transactionState));

            Monitor.Exit(this);
        }

        /*
         1: check if the lease was assigned to this TM. If it was, add it to list of leases held.
         2: check if any of the permissions of the lease was assigned to somebody else, if so we remove it from our list of leases held.
         3: nothing happens if leases w/ conflicting permissions are assigned in the same slot. Check and wait for the other TM to execute it.
         */
        private void CheckLeaseConflicts(StatusUpdateResponse statusUpdateResponse)
        {
            var leasesHeldBeforeRunning = _leasesHeld;
            foreach (var lease in statusUpdateResponse.Leases)
            {
                // if the lease was assigned to this TM, add to leases held
                if (lease.Id == _processId)
                {
                    _leasesHeld.Add(lease);
                    foreach (TransactionState transactionState in _transactionsState)
                    {
                        // if the lease is in the list of leases required to execute the transaction, remove it from the list
                        transactionState.Permissions.Where(perm => lease.Permissions.Contains(perm.Item1)).ToList().ForEach(perm => perm.Item2 = true);
                    }
                }
                else
                {
                    // if the lease was in leasesHeldBeforeRunning, remove it from leases held
                    // this guarantees that leases assigned in the same slot aren't removed
                    if (leasesHeldBeforeRunning.Contains(lease))
                    {
                        _leasesHeld.RemoveAll(leaseHeld => leaseHeld.Permissions.Contains(lease.Permissions[0]));
                    }
                }
            }
        }

        public bool SuspicionConsensus(string suspectedId)
        {
            int agreeingProcesses = 1;
            var tasks = new List<Task>();

            if (_reachableTMs.Count < _transactionManagers.Count / 2)
            {
                Console.WriteLine("Not enough processes to reach a majority, aborting update...");
                throw new MajorityInsufficiencyException();
            }

            foreach (var host in _transactionManagers.Where(host => _reachableTMs.Contains(host.Key)))
            {
                var t = Task.Run(() =>
                {
                    SuspicionConsensusRequest suspicionRequest = new SuspicionConsensusRequest { SuspectedId = suspectedId };
                    SuspicionConsensusResponse suspicionResponse = _transactionManagers[host.Key].SuspicionCheck(suspicionRequest);
                    if (suspicionResponse.Opinion == true) { agreeingProcesses++; }

                    return Task.CompletedTask;
                }, _cancelPreviousActiveTasks.Token);
                tasks.Add(t);
            }

            try
            {
                for (var i = 0; i < _transactionManagers.Count / 2; i++)
                {
                    tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Task was canceled (most likely) due to a new slot having started.");
                throw new SlotExecutionTimeoutException();
            }

            return agreeingProcesses > _transactionManagers.Count / 2;
        }

        public SuspicionConsensusResponse SuspicionCheck(SuspicionConsensusRequest request)
        {
            bool opinion = _tmsSuspectedPerSlot[_currentSlot].Contains(request.SuspectedId);
            return new SuspicionConsensusResponse { Opinion = opinion };
        }

        public void ExecuteTransaction(TransactionState transactionState)
        {
            Console.WriteLine($"Executing transaction...");
            foreach (var dadintKey in transactionState.Request.Reads)
            {
                if (_transactionManagerDadInts.TryGetValue(dadintKey, out var dadint))
                    _dadIntsRead.Add(dadint);
                else
                {
                    // if dadint doesn't exist, create new dadInt with value = null
                    var newDadInt = new DADInt { Key = dadintKey, Value = null };
                    _dadIntsRead.Add(newDadInt);
                }
            }

            WriteTransactions(transactionState.Request.Writes);
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

        public void TwoPhaseCommit(TransactionState transactionState)
        {
            List<Task> tasks = new List<Task>();
            var prepareResponses = new List<PrepareResponse>();
            
            if (_reachableTMs.Count < _transactionManagers.Count / 2)
            {
                throw new MajorityInsufficiencyException();
            }

            PrepareRequest prepareRequest = new PrepareRequest { };
            foreach (var host in _transactionManagers.Where(host => _reachableTMs.Contains(host.Key)))
            {
                var t = Task.Run(() =>
                {
                    try
                    {
                        PrepareResponse prepareResponse = _transactionManagers[host.Key].Prepare(new PrepareRequest());
                        prepareResponses.Add(prepareResponse);
                    }
                    catch (Grpc.Core.RpcException e)
                    {
                        Console.WriteLine(e.Status);
                    }
                    return Task.CompletedTask;
                }, _cancelPreviousActiveTasks.Token);
                tasks.Add(t);
            }

            try
            {
                // wait for a majority of them to reply
                for (var i = 0; i < _transactionManagers.Count / 2; i++)
                {
                    tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Task was canceled (most likely) due to a new slot having started.");
                throw new SlotExecutionTimeoutException();
            }

            // if we reach here, a majority of responses were acquired and no exception thrown

            tasks.Clear();
            // send commit to all TMs
            foreach (var host in _transactionManagers.Where(host => _reachableTMs.Contains(host.Key)))
            {
                var t = Task.Run(() =>
                {
                    try
                    {
                        CommitRequest commitRequest = new();
                        commitRequest.Writes.AddRange(transactionState.Request.Writes);
                        CommitResponse commitResponse = _transactionManagers[host.Key].Commit(commitRequest);
                    }
                    catch (Grpc.Core.RpcException e)
                    {
                        Console.WriteLine(e.Status);
                    }
                    return Task.CompletedTask;
                }, _cancelPreviousActiveTasks.Token);
                tasks.Add(t);
            }

            try
            {
                // wait for a majority of them to reply
                for (var i = 0; i < _transactionManagers.Count / 2; i++)
                {
                    tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Task was canceled (most likely) due to a new slot having started.");
                throw new SlotExecutionTimeoutException();
            }

            // if we get here we execute the transaction
            ExecuteTransaction(transactionState);
        }

        public PrepareResponse ReplyWithPrepare()
        {
            // if we get prepare request, we reply with ok
            return new PrepareResponse { };
        }

        public CommitResponse CommitRequestReceived(CommitRequest commitRequest)
        {
            // if we get commit request, we execute the transactions received
            WriteTransactions(commitRequest.Writes);
            return new CommitResponse { Ok = true };
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

            if (_reachableTMs.Count < _transactionManagers.Count / 2)
            {
                Console.WriteLine("Not enough processes to reach a majority, aborting update...");
                throw new MajorityInsufficiencyException();
            }

            foreach (var host in _transactionManagers.Where(host => _reachableTMs.Contains(host.Key) || host.Key == _processId))
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
                }, _cancelPreviousActiveTasks.Token);
                tasks.Add(t);
            }

            try
            {
                // Wait for a majority of them to reply (+self response)
                for (var i = 0; i < _transactionManagers.Count / 2 + 1; i++)
                {
                    tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Task was canceled (most likely) due to a new slot having started.");
                throw new SlotExecutionTimeoutException();
            }

            Monitor.Enter(this);
            // group responses by size and get the biggest group
            var biggestLog = responses.GroupBy(response => response.Writes.Count).OrderByDescending(group => group.Count()).First().ToList().MostCommon();
            Monitor.Exit(this);

            // if the biggest one is different from ours and if they logs is not empty, we need to update our log to the latest one
            var sortedBiggestLog = biggestLog.Writes.OrderBy(dadint => dadint.Key).ToList();
            var sortedWriteLog = _writeLog.OrderBy(dadint => dadint.Key).ToList();

            if (!sortedBiggestLog.SequenceEqual(sortedWriteLog) && biggestLog.Writes.Count != 0)
            {
                UpdateLocalLog(biggestLog);
            }
        }

        private void UpdateLocalLog(UpdateResponse updateResponse)
        {
            Monitor.Enter(this);

            _leasesHeld = new List<Lease>();
            _transactionManagerDadInts = new Dictionary<string, DADInt>();
            _dadIntsRead = new List<DADInt>();
            _transactionsState.ForEach(_leasesHeld => _leasesHeld.Permissions.ForEach(perm => perm.Item2 = false)); // reset permissions for transactions
            _writeLog = new List<DADInt>();
            WriteTransactions(updateResponse.Writes);

            Console.WriteLine($"Updated local log to the latest one!");

            Monitor.Exit(this);
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
            // go to sleep
            Monitor.Enter(this);

            /* get the transactionState that has the lease that is in the request
             When that transactionState is not in _transactionsState anymore, we remove the lease in the request from _leasesHeld
             */
            var transactionState = _transactionsState.Find(transactionState => transactionState.Permissions
              .Select(perm => perm.Item1).Contains(request.Lease.Permissions[0]));
            while (_transactionsState.Contains(transactionState))
            {
                // thread will be woken up by the monitorpulseall in the execute transaction method
                Monitor.Wait(this);
            }

            // remove the lease in the request from _leasesHeld (a single lease)
            _leasesHeld.RemoveAll(leaseHeld => leaseHeld.Permissions.Contains(request.Lease.Permissions[0]));

            Monitor.Exit(this);
            return new SameSlotLeaseExecutionResponse { Lease = request.Lease };
        }
    }
}