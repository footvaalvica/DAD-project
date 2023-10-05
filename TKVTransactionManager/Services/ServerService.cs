using TransactionManagerTransactionManagerProto;
using TransactionManagerLeaseManagerServiceProto;
using System.Data;
using System.Globalization;
using ClientTransactionManagerProto;
using System.Security;
using System.Diagnostics;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;

namespace TKVTransactionManager.Services
{
    using LeaseManagers =
        Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient>;

    public class ServerService
    {
        // Config file variables
        private readonly string processId;
        private readonly List<List<bool>> tmsStatePerSlot; // all TMs states per slot

        private readonly List<List<string>>
            tmsSuspectedPerSlot; // processes that this TM suspects to be crashed PER slot

        private readonly Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers;
        private readonly LeaseManagers leaseManagers; // TODO: fix the service / see if its correct
        private readonly int processIndex;

        // Paxos variables
        private bool isCrashed;
        private int currentSlot; // The number of experienced slots (process may be frozen and not experience all slots)

        // Replication variables
        private Dictionary<string, DADInt> transactionManagerDadInts;

        private List<Lease> leasesHeld;
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
            this.processId = processId;
            this.transactionManagers = transactionManagers;
            this.leaseManagers = leaseManagers;
            this.tmsStatePerSlot = tmsStatePerSlot;
            this.tmsSuspectedPerSlot = tmsSuspectedPerSlot;
            this.processIndex = processIndex;

            this.isCrashed = false;
            this.currentSlot = 0;
            this.transactionManagerDadInts = new();
            this.leasesHeld = new List<Lease>();
        }
        // TODO : etc...

        public void PrepareSlot()
        {
            Monitor.Enter(this);

            // End of slots
            if (this.currentSlot >= tmsStatePerSlot.Count)
            {
                Console.WriteLine("Slot duration ended but no more slots to process.");
                return;
            }

            Console.WriteLine($"Preparing slot... ------------------------------------------");

            // get process state
            this.isCrashed = tmsStatePerSlot[this.currentSlot][processIndex];

            Console.WriteLine($"Process is now {(this.isCrashed ? "crashed" : "normal")}");

            // Global slot counter
            this.currentSlot++;

            // If process is crashed, don't do anything
            if (this.isCrashed)
            {
                Console.WriteLine("Ending preparation -----------------------");
                Monitor.Exit(this);
                return;
            }

            Monitor.PulseAll(this);

            askForLeaseManagersStatus();

            Monitor.Exit(this);
        }

        public StatusResponse Status(StatusRequest statusRequest)
        {
            return new StatusResponse { Status = true };
        }

        public TransactionResponse TxSubmit(TransactionRequest transactionRequest)
        {
            List<string> leasesRequired = new List<string>();
            List<DADInt> dadIntsRead = new List<DADInt>();
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

            // TODO lease managers knowing about other projects' leases and stuffs (maybe)

            var allLeases = true;
            bool found;
            foreach (var dadint in leasesRequired)
            {
                found = false;
                foreach (var lease in leasesHeld)
                {
                    if (lease.Permissions.Contains(dadint))
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                    allLeases = false;
                break;
            }

            if (!allLeases)
            {
                Console.WriteLine($"Requesting leases...");
                var lease = new Lease { Id = processId };
                lease.Permissions.AddRange(leasesRequired);
                var leaseRequest = new LeaseRequest { Slot = currentSlot, Lease = lease };
                StatusUpdateResponse? statusUpdateResponse = null;

                var tasks = new List<Task>();
                foreach (var host in leaseManagers)
                {
                    var t = Task.Run(() =>
                    {
                        try
                        {
                            Console.WriteLine("sending leaser request");
                            leaseManagers[host.Key].Lease(leaseRequest);
                        }
                        catch (Grpc.Core.RpcException e)
                        {
                            Console.WriteLine(e.Status);
                        }
                        
			statusUpdateResponse = leaseManagers[host.Key].StatusUpdate(new Empty());
                        return Task.CompletedTask;
                    });
                    tasks.Add(t);
                }

                Task.WaitAny(tasks.ToArray());
                
                if (statusUpdateResponse.Status)
                { 
                    allLeases = true; 
                    leasesHeld.Add(lease);
                }
                else
                {
                    Console.WriteLine("Failed to acquire leases!"); // TODO
                }
            }

            if (allLeases)
            {
                Console.WriteLine($"Lease granted!");
                foreach (var dadintKey in transactionRequest.Reads)
                {
                    if (transactionManagerDadInts.TryGetValue(dadintKey, out var dadint))
                        dadIntsRead.Add(dadint);
                    else
                    {
                        Console.WriteLine("Requested read on non-existing DADINT."); // TODO
                    }
                }
                
		foreach (var dadint in transactionRequest.Writes)
                {
                    if (transactionManagerDadInts.ContainsKey(dadint.Key))
                        transactionManagerDadInts[dadint.Key].Value = dadint.Value;
                    else
                    {
                        Console.WriteLine("Requested write on non-existing DADINT."); // TODO
                    }
                }
            }

            Console.WriteLine($"Finished processing transaction request...");
            var transactionResponse = new TransactionResponse();
            transactionResponse.Response.AddRange(dadIntsRead);
            return transactionResponse;
        }

        public void askForLeaseManagersStatus()
        {
            StatusUpdateResponse statusUpdateResponse = new StatusUpdateResponse();
            var tasks = new List<Task>();
            foreach (var host in leaseManagers)
            {
                var t = Task.Run(() =>
                {
                    statusUpdateResponse = leaseManagers[host.Key].StatusUpdate(new Empty());
                    return Task.CompletedTask;
                });
                tasks.Add(t);
            }

            Task.WaitAny(tasks.ToArray());

            bool leaseRemoved = false;

            // we are going to check for conflict leases. a lease is in conflict if a TM holds a lease for a key that was given as permission to another TM in a later Lease.
            // if the lease is in conflict, this TM should release the Lease it holds. 

            if (statusUpdateResponse.Status)
            {
                foreach (Lease lease in statusUpdateResponse.Leases)
                {
                    // Check if the lease is held by another process (TM)
                    if (lease.Id != processId)
                    {
                        // Iterate through leases held by the current process
                        foreach (Lease heldLease in leasesHeld.ToList())
                        {
                            // Check for conflicting permissions
                            if (lease.Permissions.Intersect(heldLease.Permissions).Any())
                            {
                                // Remove conflicting lease
                                leasesHeld.Remove(heldLease);
                                leaseRemoved = true;

                                // Exit inner loop since a conflicting lease was removed
                                break;
                            }
                        }

                        // Exit outer loop if a conflicting lease was removed
                        if (leaseRemoved)
                        {
                            break;
                        }
                    }
                }
            }
        }
    }
}
