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
            TimeSpan timeToGo = time - DateTime.Now.TimeOfDay;
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
                leaseManagerService.PrepareInstance();
            }, null, (int)timeToGo.TotalMilliseconds, slotDuration);
        }

        static void Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Command Line Arguments
            int processId = int.Parse(args[0]);
            string host = args[1];
            int port = int.Parse(args[2]);

            // Data from config file
            leaseManagerBankConfig config = Common.ReadConfig();

            // Process data from config file to send to serverService
            int numberOfProcesses = config.NumberOfProcesses;
            (int slotDuration, TimeSpan startTime) = config.SlotDetails;
            Dictionary<int, Paxos.PaxosClient> leaseManagerHosts = config.leaseManagerProcesses.ToDictionary(
                key => key.Id,
                // !! not sure if this cast is alright? should be tho
                value => new Paxos.PaxosClient(GrpcChannel.ForAddress(value.Address as string))
            );
            List<Dictionary<int, bool>> processesSuspectedPerSlot = config.ProcessStates.Select(states =>
            {
                return states.ToDictionary(key => key.Key, value => value.Value.Suspected);
            }).ToList();
            List<bool> processFrozenPerSlot = config.ProcessStates.Select(states => states[processId].Frozen).ToList();

            // A process should not suspect itself (it knows if its frozen or not)
            for (int i = 0; i < processesSuspectedPerSlot.Count; i++)
                processesSuspectedPerSlot[i][processId] = processFrozenPerSlot[i];

            LeaseManagerService leaseManagerService = new(processId, processFrozenPerSlot, processesSuspectedPerSlot, leaseManagerHosts);

            Server server = new()
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

            server.ShutdownAsync().Wait();
        }
    }
}