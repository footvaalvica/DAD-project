using TKVTransactionManager.Services;
using Utilities;
using Grpc.Net.Client;
using Grpc.Core;
using ClientTransactionManagerProto;
using TransactionManagerTransactionManagerProto;
using TransactionManagerLeaseManagerServiceProto;

namespace TKVTransactionManager
{
    internal class Program
    {
        static System.Threading.Timer timer;

        // TODO: change this back before submitting

        static private void SetSlotTimer(TimeSpan time, int slotDuration, ServerService serverService)
        {
            TimeSpan timeToGo = TimeSpan.Zero; //  time - DateTime.Now.TimeOfDay; // TODO: remove before submission
            if (timeToGo < TimeSpan.Zero)
            {
                Console.WriteLine("Slot starting before finished server setup.");
                Console.WriteLine("Aborting...");
                Environment.Exit(0);
                return;
            }

            // A thread will be created at timeToGo and after that, every slotDuration
            timer = new System.Threading.Timer(x =>
            {
                serverService.PrepareSlot();
            }, null, (int)timeToGo.TotalMilliseconds, slotDuration);
        }

        static void Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Command Line Arguments
            if (args.Length < 3)
            {
                Console.WriteLine("You need to pass at least 3 arguments: processId, host and port.");
                return;
            }
            string processId = args[0];
            string host = args[1];
            int port = int.Parse(args[2]);
            bool debug = args.Length > 3 && args[3].Equals("debug");

            // Data from config file
            TKVConfig config = Common.ReadConfig();

            // Process data from config file to send to serverService
            int numberOfProcesses = config.NumberOfProcesses;
            (int slotDuration, TimeSpan startTime) = config.SlotDetails;

            // TransactionM <-> TransactionM
            Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers = config.TransactionManagers.ToDictionary(
                key => key.Id,
                value => new TwoPhaseCommit.TwoPhaseCommitClient(GrpcChannel.ForAddress(value.Url))
            );
            // TransactionM <-> LeaseM
            Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient> leaseManagers = config.LeaseManagers.ToDictionary(
                key => key.Id,
                value => new TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient(GrpcChannel.ForAddress(value.Url))
            );

            List<ProcessState> statePerSlot = new List<ProcessState>(); // Podia so ir buscar sempre ao dictionary ig
            foreach (Dictionary<string, ProcessState> dict in config.ProcessStates)
            {
                if (dict != null)
                {
                    dict.TryGetValue(processId, out ProcessState processState);
                    statePerSlot.Add(processState);
                }
                else
                {
                    statePerSlot.Add(statePerSlot.Last()); // Podia deixar so a null
                }
            }

            // TODO: Check if this is correct
            //List<bool> processCrashedPerSlot = config.ProcessStates.Select(states => states[processId].Crashed).ToList();

            // A process should not suspect itself (it knows if its frozen or not) // ^^^^^^ get the stuff correct first
            //for (int i = 0; i < processesSuspectedPerSlot.Count; i++)
            //    processesSuspectedPerSlot[i][processId] = processFrozenPerSlot[i];

            ServerService serverService = new(processId, transactionManagers, leaseManagers); // processCrashedPerSlot, processesSuspectedPerSlot, 

            Server server = new Server
            {
                Services = {
                    Client_TransactionManagerService.BindService(new TMService(serverService)),
                    TwoPhaseCommit.BindService(new TwoPhaseCommitService(serverService)),
                },
                Ports = { new ServerPort(host, port, ServerCredentials.Insecure) }
            };

            server.Start();

            Console.WriteLine($"Transaction Manager with id ({processId}) listening on port {port}");
            Console.WriteLine($"First slot starts at {startTime} with intervals of {slotDuration} ms");
            Console.WriteLine($"Working with {transactionManagers.Count} TMs"); //  and {leaseManagers.Count} boney processes

            // Starts a new thread for each slot
            SetSlotTimer(startTime, slotDuration, serverService);

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}