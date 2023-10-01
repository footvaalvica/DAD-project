using Grpc.Core;
using Grpc.Net.Client;
using LeaseManagerLeaseManagerServiceProto;
using TKVLeaseManager.Services;
using TransactionManagerLeaseManagerServiceProto;
using Utilities;

namespace TKVLeaseManager
{
    internal class Program
    {
        static System.Threading.Timer timer;
        private static void SetSlotTimer(TimeSpan time, int slotDuration, LeaseManagerService leaseManagerService)
        {
            TimeSpan timeToGo = TimeSpan.Zero; /*time - DateTime.Now.TimeOfDay;*/ // TODO: remove before submission
            if (timeToGo < TimeSpan.Zero)
            {
                Console.WriteLine("Slot starting before finished server setup.");
                Console.WriteLine("Aborting...");
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
            bool debug = args.Length > 3 && args[3].Equals("debug");

            // Data from config file
            TKVConfig config = Common.ReadConfig();

            // Process data from config file to send to serverService
            int numberOfProcesses = config.NumberOfProcesses;
            (int slotDuration, TimeSpan startTime) = config.SlotDetails;
            Dictionary<string, Paxos.PaxosClient> leaseManagerHosts = config.LeaseManagers.ToDictionary(
                key => key.Id,
                value => new Paxos.PaxosClient(GrpcChannel.ForAddress(value.Url))
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
            
            ////var processesSuspectedPerSlot = config.ProcessStates.Select(states => states[processId.ToString()].Suspects).ToList();
            ////var processCrashedPerSlot = config.ProcessStates.Select(states => states[processId.ToString()].Crashed).ToList();


            ////A process should not suspect itself(it knows if its Crashed or not)
            ////for (var i = 0; i < processesSuspectedPerSlot.Count; i++)
            ////    processesSuspectedPerSlot[i][processId.ToString()] = processCrashedPerSlot[i];

            LeaseManagerService leaseManagerService = new(processId, statePerSlot, leaseManagerHosts);

            Server server = new Server
            {
                Services = {
                    Paxos.BindService(new PaxosService(leaseManagerService)),
                    TransactionManager_LeaseManagerService.BindService(new RequestLeaseService(leaseManagerService))
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

            server.ShutdownAsync().Wait(); // why was this commented out?
        }
    }
}