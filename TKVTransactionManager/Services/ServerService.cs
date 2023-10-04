using TransactionManagerTransactionManagerProto;
using TransactionManagerLeaseManagerServiceProto;
using System.Data;
using System.Globalization;
using ClientTransactionManagerProto;
using System.Security;
using System.Diagnostics;
using System.Threading.Tasks;

namespace TKVTransactionManager.Services
{
    using LeaseManagers = Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient>;
    public class ServerService
    {
        // Config file variables
        private readonly string processId;
        private readonly List<List<bool>> tmsStatePerSlot; // all TMs states per slot
        private readonly List<List<string>> tmsSuspectedPerSlot; // processes that this TM suspects to be crashed PER slot
        private readonly Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers;
        private readonly LeaseManagers leaseManagers; // TODO: fix the service / see if its correct
        private readonly int processIndex;

        // Paxos variables
        private bool isCrashed;
        private int currentSlot;  // The number of experienced slots (process may be frozen and not experience all slots)

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
            foreach (string dadintKey in transactionRequest.Reads)
            {
                Console.WriteLine($"     DADINT2READ: {dadintKey}");
                // add to leasesRequired
                leasesRequired.Add(dadintKey);
            }
            foreach (DADInt dadint in transactionRequest.Writes)
            {
                Console.WriteLine($"     DADINT2RWRITE: {dadint.Key}:{dadint.Value}");
                leasesRequired.Add(dadint.Key);
            }

            bool allLeases = true;
            bool found;
            foreach (string dadint in leasesRequired)
            {
                found = false;
                foreach (Lease lease in leasesHeld)
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
                Lease lease = new Lease { Id = processId };
                lease.Permissions.AddRange(leasesRequired);
                LeaseRequest leaseRequest = new LeaseRequest { Lease = lease };
                LeaseResponse leaseResponse = null;

                List<Task> tasks = new List<Task>();
                foreach (var host in this.leaseManagers)
                {
                    Task t = Task.Run(() =>
                    {
                        try
                        {
                            leaseResponse = leaseManagers[host.Key].Lease(leaseRequest);
                        }
                        catch (Grpc.Core.RpcException e)
                        {
                            Console.WriteLine(e.Status);
                        }
                        return Task.CompletedTask;
                    });
                    tasks.Add(t);
                }
                Task.WaitAny(tasks.ToArray());
                
                if (leaseResponse.Status)
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
                foreach (string dadintKey in transactionRequest.Reads)
                {
                    if (transactionManagerDadInts.TryGetValue(dadintKey, out DADInt dadint))
                        dadIntsRead.Add(dadint);
                    else
                    {
                        Console.WriteLine("Requested read on non-existing DADINT."); // TODO
                    }
                }
                foreach (DADInt dadint in transactionRequest.Writes)
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
            TransactionResponse transactionResponse = new TransactionResponse();
            transactionResponse.Response.AddRange(dadIntsRead);
            return transactionResponse;
        }
    }
}