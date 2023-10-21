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
        static Timer timer;

        // TODO: change this back before submitting

        static private void SetSlotTimer(TimeSpan time, int slotDuration, ServerService serverService)
        {
            TimeSpan timeToGo = time - DateTime.Now.TimeOfDay;
            if (timeToGo < TimeSpan.Zero)
            {
                Console.WriteLine("Slot starting before finished server setup. Aborting...");
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
            TimeSpan startTime = TimeSpan.Parse(args[1]);
            string host = args[2];
            int port = int.Parse(args[3]);
            bool debug = args.Length > 4 && args[4].Equals("debug");

            // Data from config file
            TKVConfig config = Common.ReadConfig();

            // Process data from config file to send to serverService
            int numberOfProcesses = config.NumberOfProcesses;
            (int slotDuration, _) = config.SlotDetails;

            // TransactionManager <-> TransactionManager 
            Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers = config.TransactionManagers.ToDictionary(
                key => key.Id,
                value => new TwoPhaseCommit.TwoPhaseCommitClient(GrpcChannel.ForAddress(value.Url))
            );
            // TransactionManager  <-> LeaseManager 
            Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient> leaseManagers = config.LeaseManagers.ToDictionary(
                key => key.Id,
                value => new TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient(GrpcChannel.ForAddress(value.Url))
            );

            List<ProcessState> statePerSlot = new List<ProcessState>();
            foreach (Dictionary<string, ProcessState> dict in config.ProcessStates)
            {
                if (dict != null)
                {
                    dict.TryGetValue(processId, out ProcessState processState);
                    statePerSlot.Add(processState);
                }
                else
                {
                    statePerSlot.Add(statePerSlot.Last());
                }
            }

            List<List<bool>> tmsStatePerSlot = new List<List<bool>>();
            List<List<string>> tmsSuspectedPerSlot = new List<List<string>>();


            for (int i = 0; i < config.ProcessStates.Length; i++)
            {
                tmsStatePerSlot.Add(new List<bool>());
                tmsSuspectedPerSlot.Add(new List<string>());

                if (config.ProcessStates[i] == null)
                {
                    tmsStatePerSlot[i] = tmsStatePerSlot[i - 1];
                    tmsSuspectedPerSlot[i] = tmsSuspectedPerSlot[i - 1];
                    continue;
                }
                
                else
                {
                    for (int j = 0; j < config.TransactionManagers.Count; j++)
                    {
                        tmsStatePerSlot[i].Add(config.ProcessStates[i][config.TransactionManagers[j].Id].Crashed);
                    }
                    tmsSuspectedPerSlot[i] = config.ProcessStates[i][processId].Suspects; // getting the tms that each tm suspects PER slot. i = slotId
                }

            }
            int processIndex = config.TransactionManagers.FindIndex(x => x.Id == processId);

            ServerService serverService = new(processId, transactionManagers, leaseManagers, tmsStatePerSlot, tmsSuspectedPerSlot, processIndex);

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
            Console.WriteLine($"Working with {transactionManagers.Count} TMs");

            // Starts a new thread for each slot
            SetSlotTimer(startTime, slotDuration, serverService);

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}