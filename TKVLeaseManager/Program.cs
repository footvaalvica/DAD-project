﻿using Grpc.Core;
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
        private static void SetInstanceTimer(TimeSpan time, int instanceDuration, LeaseManagerService leaseManagerService)
        {
            TimeSpan timeToGo = time - DateTime.Now.TimeOfDay;
            if (timeToGo < TimeSpan.Zero)
            {
                Console.WriteLine("Instance starting before finished server setup.");
                Console.WriteLine("Aborting...");
                Environment.Exit(0);
                return;
            }

            // A thread will be created at timeToGo and after that, every instanceDuration
            timer = new Timer(x =>
            {
                leaseManagerService.PrepareInstance();
            }, null, (int)timeToGo.TotalMilliseconds, instanceDuration);
        }

        static void Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Command Line Arguments
            int processId = int.Parse(args[0]);
            string host = args[1];
            int port = int.Parse(args[2]);

            // Data from config file
            TKVConfig config = Common.ReadConfig();

            // Process data from config file to send to serverService
            int numberOfProcesses = config.NumberOfProcesses;
            (int instanceDuration, TimeSpan startTime) = config.SlotDetails;
            Dictionary<string, Paxos.PaxosClient> leaseManagerHosts = config.LeaseManagers.ToDictionary(
                key => key.Id,
                // !! not sure if this cast is alright? should be tho
                value => new Paxos.PaxosClient(GrpcChannel.ForAddress(value.Id))
            );
            
            var processesSuspectedPerInstance = config.ProcessStates.Select(states => states[processId.ToString()].Suspects).ToList();
            var processCrashedPerInstance = config.ProcessStates.Select(states => states[processId.ToString()].Crashed).ToList();

            Console.WriteLine(processesSuspectedPerInstance);

            // A process should not suspect itself (it knows if its Crashed or not)
            ////for (var i = 0; i < processesSuspectedPerInstance.Count; i++)
            ////    processesSuspectedPerInstance[i][processId.ToString()] = processCrashedPerInstance[i];

            ////LeaseManagerService leaseManagerService = new(processId, processCrashedPerInstance, processesSuspectedPerInstance, leaseManagerHosts);

            ////Server server = new()
            ////{
            ////    Services = {
            ////        Paxos.BindService(new PaxosService(leaseManagerService)),
            ////        TransactionManager_LeaseManagerService.BindService(new RequestLeaseService(leaseManagerService))
            ////    },
            ////    Ports = { new ServerPort(host, port, ServerCredentials.Insecure) }
            ////};

            ////server.Start();

            ////Console.WriteLine($"leaseManager ({processId}) listening on port {port}");
            ////Console.WriteLine($"First instance starts at {startTime} with intervals of {instanceDuration} ms");
            ////Console.WriteLine($"Working with {leaseManagerHosts.Count} leaseManager processes");

            ////// Starts thread in timeSpan
            ////SetInstanceTimer(startTime, instanceDuration, leaseManagerService);

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            ////server.ShutdownAsync().Wait();
        }
    }
}