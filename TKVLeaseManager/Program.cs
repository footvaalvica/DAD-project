using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using TKVLeaseManager.Services;
using Utilities;

// TODO!

namespace TKVLeaseManager
{
    internal class Program
    {
        private static void SetSlotTimer(TimeSpan time, int slotDuration, LeaseManagerService leaseManagerService)
        {
            var timeToGo = time - DateTime.Now.TimeOfDay;
            if (timeToGo < TimeSpan.Zero)
            {
                ////find better messages
                Console.WriteLine("Slot starting before finished server setup.");
                Console.WriteLine("Aborting...");
                Environment.Exit(0);
                return;
            }

            // A thread will be created at timeToGo and after that, every slotDuration
            var timer = new System.Threading.Timer(x => { leaseManagerService.PrepareSlot(); }, null, timeToGo, TimeSpan.FromMilliseconds(slotDuration));
        }

        private static void Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Command Line Arguments
            var processId = int.Parse(args[0]);
            var host = args[1];
            var port = int.Parse(args[2]);

            // Data from config file
            TkvTransactionManagerConfig config = Common.ReadConfig();

            // Process data from config file to send to leaseManagerService
            int numberOfProcesses = config.NumberOfProcesses;
            (int slotDuration, TimeSpan startTime) = config.SlotDetails;
            Dictionary<int, Paxos.PaxosClient> boneyHosts = config.BoneyProcesses.ToDictionary(
                key => key.Id,
                value => new Paxos.PaxosClient(GrpcChannel.ForAddress(value.Address))
            );
            List<Dictionary<int, bool>> processesSuspectedPerSlot = config.ProcessStates.Select(states =>
            {
                return states.ToDictionary(key => key.Key, value => value.Value.Suspected);
            }).ToList();
            List<bool> processFrozenPerSlot = config.ProcessStates.Select(states => states[processId].Frozen).ToList();


            // A process should not suspect itself (it knows if its frozen or not)
            for (var i = 0; i < processesSuspectedPerSlot.Count; i++)
                processesSuspectedPerSlot[i][processId] = processFrozenPerSlot[i];

            var serverService = new ServerService(processId, processFrozenPerSlot, processesSuspectedPerSlot, boneyHosts);

            var server = new Server
            {
                Services = {
                    Paxos.BindService(new PaxosService(serverService)),
                },
                Ports = { new ServerPort(host, port, ServerCredentials.Insecure) }
            };

            server.Start();

            Console.WriteLine($"TKVLeaseManager ({processId}) listening on port {port}");
            Console.WriteLine($"First slot starts at {startTime} with intervals of {slotDuration} ms");
            Console.WriteLine($"Working with {tkvLeaseManager.Count} TKVLeaseManager processes");

            // Starts thread in timeSpan
            SetSlotTimer(startTime, slotDuration, serverService);

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}