using TransactionManagerTransactionManagerProto;
using TransactionManagerLeaseManagerServiceProto;
using ClientTransactionManagerProto;
using Google.Protobuf.WellKnownTypes;

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
        private readonly LeaseManagers _leaseManagers; // TODO: fix the service / see if its correct
        private readonly int _processIndex;

        // Paxos variables
        private bool _isCrashed;
        private int _currentSlot; // The number of experienced slots (process may be frozen and not experience all slots)

        // Replication variables
        private Dictionary<string, DADInt> _transactionManagerDadInts;

        private List<Lease> _leasesHeld;

        private bool _allLeases;

        private List<DADInt> _dadIntsRead;

        // TODO this assumes sequential transactions, which might be the case? I don't know
        private List<TransactionState> _transactionsState;
        //private readonly Dictionary<(int, int), ClientCommand> tentativeCommands; // key: (clientId, clientSequenceNumber)
        //private readonly Dictionary<(int, int), ClientCommand> committedCommands;

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
        
        // TODO why statusrequest not used? should be empty then!
        public StatusResponse Status(StatusRequest statusRequest)
        {
            if (_isCrashed) { Monitor.Wait(this); }
            return new StatusResponse { Status = true };
        }

        public TransactionResponse TxSubmit(TransactionRequest transactionRequest)
        {
            if (_isCrashed) { Monitor.Wait(this); }

            var leasesRequired = new List<string>();
            _dadIntsRead = new List<DADInt>(); // TODO ??

            TransactionState transactionState = new TransactionState { Leases = new(), Request = transactionRequest };

            Console.WriteLine($"Received transaction request: ");
            Console.WriteLine($"     FROM: {transactionRequest.Id}");

            foreach (var dadintKey in transactionRequest.Reads)
            {
                Console.WriteLine($"     DADINT2READ: {dadintKey}");
                // add to leasesRequired
                leasesRequired.Add(dadintKey);
            }

            foreach (var dadint in transactionRequest.Writes)
            {
                Console.WriteLine($"     DADINT2RWRITE: {dadint.Key}:{dadint.Value}");
                leasesRequired.Add(dadint.Key);
            }

            // check if has all leases
            transactionState.Leases = leasesRequired
                .Where(lease => !_leasesHeld.Any(leaseHeld => leaseHeld.Permissions.Contains(lease)))
                .ToList();

            // TODO lease managers knowing about other projects' leases and stuffs (maybe)

            // if it doesn't have all leases, request them
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
                            Console.WriteLine("sending lease request");
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

            // TODO when replying we should compare slot of the request with the current slot and reply accordingly
            transactionResponse.Response.AddRange(_dadIntsRead); // TODO: dadIntsRead is empty
            return transactionResponse;
        }

        // TODO this function should store things globally
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
            Task.WaitAny(tasks.ToArray());

            Monitor.Enter(this);

            Console.WriteLine($"Received status update from lease managers");

            Console.WriteLine($"    Got ({statusUpdateResponse.Leases.Count}) Leases.");

            // we are going to check for conflict leases. a lease is in conflict if a TM holds a lease for a key that was given as permission to another TM in a later Lease.
            // if the lease is in conflict, this TM should release the Lease it holds. 
            foreach (var lease in statusUpdateResponse.Leases)
            {
                var leaseRemoved = false;
                // Check if the lease is held by another process (TM)
                if (lease.Id != _processId) continue;
                // Iterate through leases held by the current process
                foreach (var heldLease in _leasesHeld.ToList().Where(heldLease => lease.Permissions.Intersect(heldLease.Permissions).Any()))
                {
                    // Remove conflicting lease
                    _leasesHeld.Remove(heldLease);
                    leaseRemoved = true;

                    // Exit inner loop since a conflicting lease was removed
                    break;
                }

                Console.WriteLine("adding received lease");
                _leasesHeld.Add(lease);

                // Exit outer loop if a conflicting lease was removed // why?
                ////if (leaseRemoved) { continue; }

                foreach (TransactionState transactionState in _transactionsState.Where(_transactionsState => _transactionsState.Leases.Count > 0))
                {
                    transactionState.Leases.RemoveAll(leaseRequest => lease.Permissions.Contains(leaseRequest));
                }
            }

            // transaction execution
            // not needed for this checkpoint
            ////foreach (TransactionState transactionState in _transactionsState
            ////    .Where(_transactionsState => _transactionsState.Leases.Count == 0))
            ////{
            ////    Console.WriteLine($"Lease granted!");
            ////    foreach (var dadintKey in transactionState.Request.Reads)
            ////    {
            ////        if (_transactionManagerDadInts.TryGetValue(dadintKey, out var dadint))
            ////            _dadIntsRead.Add(dadint);
            ////        else
            ////        {
            ////            Console.WriteLine("Requested read on non-existing DADINT."); // TODO
            ////        }
            ////    }

            ////    foreach (var dadint in transactionState.Request.Writes)
            ////    {
            ////        if (_transactionManagerDadInts.TryGetValue(dadint.Key, out var j))
            ////            j.Value = dadint.Value;
            ////        else
            ////        {
            ////            Console.WriteLine("Requested write on non-existing DADINT."); // TODO
            ////        }
            ////    }
            ////}
            ////_transactionsState.RemoveAll(transactionState => transactionState.Leases.Count == 0);

            Monitor.Exit(this);
        }
    }
}