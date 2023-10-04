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
    using LeaseManagers = Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient>;
    public class ServerService
    {
        // Config file variables
        private readonly string processId;
        private readonly List<bool> processesCrashedPerSlot;
        private readonly List<Dictionary<int, bool>> processesSuspectedPerSlot;
        private readonly Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers;
        private readonly Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient> leaseManagers; // TODO: fix the service / see if its correct

        // Paxos variables
        private bool isCrashed;
        private int totalSlots;   // The number of total slots elapsed since the beginning of the program
        private int currentSlot;  // The number of experienced slots (process may be frozen and not experience all slots)
        private readonly Dictionary<int, int> primaryPerSlot;

        // Replication variables
        private decimal balance;
        private bool isCleaning;
        private int currentSequenceNumber;
        private Dictionary<string, DADInt> transactionManagerDadInts;
        private List<Lease> leasesHeld;
        //private readonly Dictionary<(int, int), ClientCommand> tentativeCommands; // key: (clientId, clientSequenceNumber)
        //private readonly Dictionary<(int, int), ClientCommand> committedCommands;

        public ServerService(
            string processId,
            //List<bool> processCrashedPerSlot,
            //List<Dictionary<int, bool>> processesSuspectedPerSlot,
            Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers,
            Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient> leaseManagers
            )
        {
            this.processId = processId;
            this.transactionManagers = transactionManagers;
            this.leaseManagers = leaseManagers;
            //this.processCrashedPerSlot = processCrashedPerSlot;
            //this.processesSuspectedPerSlot = processesSuspectedPerSlot;

            isCrashed = false;
            totalSlots = 0;
            currentSlot = 0;
            currentSequenceNumber = 0;
            transactionManagerDadInts = new Dictionary<string, DADInt>();
            leasesHeld = new List<Lease>();
            primaryPerSlot = new Dictionary<int, int>();

            isCleaning = false;
            //this.tentativeCommands = new Dictionary<(int, int), ClientCommand>(); // TODO: client commands
            //this.committedCommands = new Dictionary<(int, int), ClientCommand>();
        }
        // TODO : etc...

        public void PrepareSlot()
        {
            // TODO
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
    }
}