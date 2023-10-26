using Grpc.Core;
using Grpc.Net.Client;
using LeaseManagerLeaseManagerServiceProto;
using TKVLeaseManager.Services;
using TransactionManagerLeaseManagerServiceProto;
using ClientLeaseManagerProto;
using Utilities;

namespace TKVLeaseManager
{
    internal class Program
    {
        static Timer timer;
        private static void SetSlotTimer(TimeSpan time, int slotDuration, LeaseManagerService leaseManagerService)
        {
            TimeSpan timeToGo = TimeSpan.Zero;
            if (time != TimeSpan.Zero)
            {
                timeToGo = time - DateTime.Now.TimeOfDay;
            }
            if (timeToGo < TimeSpan.Zero)
            {
                Console.WriteLine("Slot starting before finished server setup. Aborting...");
                Environment.Exit(0);
                return;
            }

            // A thread will be created at timeToGo and after that, every slotDuration
            timer = new Timer(x =>
            {
                leaseManagerService.PrepareSlot();
            }, null, (int)timeToGo.TotalMilliseconds, slotDuration);
        }

        static void Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            string processId = args[0];
            string host = args[1];
            int port = int.Parse(args[2]);
            TimeSpan startTime = TimeSpan.Zero;
            if (args.Length > 3)
            {
                startTime = TimeSpan.Parse(args[3]);
            }

            // Data from config file
            TKVConfig config = Common.ReadConfig();

            // Process data from config file to send to serverService
            int numberOfProcesses = config.NumberOfProcesses;
            (int slotDuration, _) = config.SlotDetails; // Ignoring starttime from config file
            Dictionary<string, Paxos.PaxosClient> leaseManagerHosts = config.LeaseManagers.ToDictionary(
                key => key.Id,
                value => new Paxos.PaxosClient(GrpcChannel.ForAddress(value.Url))
            );

            List<List<ProcessState>> statePerSlot = new(); // LeaseManagers' state per slot
            foreach (Dictionary<string, ProcessState> dict in config.ProcessStates)
            {
                if (dict != null)
                {
                    // Get only ProcessState that are contained in config.LeaseManagers
                    statePerSlot.Add(dict.Where(x => leaseManagerHosts.ContainsKey(x.Key)).Select(x => x.Value).ToList());
                }
                else
                {
                    statePerSlot.Add(statePerSlot.Last());
                }
            }

            int processIndex = config.LeaseManagers.FindIndex(x => x.Id == processId);
            List<string> processBook = config.LeaseManagers.Select(x => x.Id).ToList();
            LeaseManagerService leaseManagerService = new(processIndex, processId, processBook, statePerSlot, leaseManagerHosts);

            Server server = new Server
            {
                Services = {
                    Paxos.BindService(new PaxosService(leaseManagerService)),
                    TransactionManager_LeaseManagerService.BindService(new RequestLeaseService(leaseManagerService)),
                    Client_LeaseManagerService.BindService(new LMService(leaseManagerService))
                },
                Ports = { new ServerPort(host, port, ServerCredentials.Insecure) }
            };

            server.Start();

            Console.WriteLine($"leaseManager ({processId}) listening on port {port}");
            Console.WriteLine($"First slot starts at {startTime} with intervals of {slotDuration} ms");
            Console.WriteLine($"Working with {leaseManagerHosts.Count} leaseManager processes");

            // Starts thread in timeSpan
            SetSlotTimer(startTime, slotDuration, leaseManagerService);

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}