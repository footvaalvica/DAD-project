﻿using TKVTransactionManager.Services;
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
        static private void SetSlotTimer(TimeSpan time, int slotDuration, ServerService serverService)
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
            TimeSpan startTime = TimeSpan.Zero;
            if (args.Length > 3)
            {
                startTime = TimeSpan.Parse(args[3]);
            }

            // Data from config file
            TKVConfig config = Common.ReadConfig();

            // Process data from config file to send to serverService
            int numberOfProcesses = config.NumberOfProcesses;
            (int slotDuration, _) = config.SlotDetails;

            // TransactionManager <-> TransactionManager 
            Dictionary<string, Gossip.GossipClient> transactionManagers = config.TransactionManagers.ToDictionary(
                key => key.Id,
                value => new Gossip.GossipClient(GrpcChannel.ForAddress(value.Url))
            );
            // TransactionManager  <-> LeaseManager 
            Dictionary<string, TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient> leaseManagers = config.LeaseManagers.ToDictionary(
                key => key.Id,
                value => new TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceClient(GrpcChannel.ForAddress(value.Url))
            );

            List<ProcessState> statePerSlot = new List<ProcessState>();
            statePerSlot.Add(new ProcessState(false, new List<string>())); // first slot is always not crashed and not suspected
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
                        tmsStatePerSlot[i].Add(statePerSlot[i].Crashed);
                    }
                    tmsSuspectedPerSlot[i] = statePerSlot[i].Suspects; // getting the tms that each tm suspects PER slot. i = slotId
                }

            }
            int processIndex = config.TransactionManagers.FindIndex(x => x.Id == processId);
            List<string> processBook = config.TransactionManagers.Select(x => x.Id).ToList();

            ServerService serverService = new(processId, transactionManagers, leaseManagers, tmsStatePerSlot, tmsSuspectedPerSlot, processIndex, processBook);

            Server server = new Server
            {
                Services = {
                    Client_TransactionManagerService.BindService(new TMService(serverService)),
                    Gossip.BindService(new GossipService(serverService)),
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