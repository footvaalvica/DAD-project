using Grpc.Net.Client;
using Utilities;
using ClientTransactionManagerProto;
using System.Diagnostics;

namespace TKVClient
{
    using TransactionManagers = Dictionary<string, Client_TransactionManagerService.Client_TransactionManagerServiceClient>;
    internal class Program
    {
        static TransactionManagers? transactionManagers = null;
        static void Wait(string[] command)
        {
            try
            {
                if (command.Length == 2)
                {
                    Console.WriteLine("Waiting for " + command[1] + " milliseconds...");
                    System.Threading.Thread.Sleep(int.Parse(command[1]));
                }
                else { Console.WriteLine("No time amount provided for wait."); }
            }
            catch (FormatException)
            {
                Console.WriteLine("Invalid time amount provided for wait.");
            }
        }

        static bool Status()
        {
            StatusRequest request = new StatusRequest();

            List<Task> tasks = new List<Task>();
            foreach (var tm in transactionManagers)
            {
                Task t = Task.Run(() =>
                {
                    try
                    {
                        StatusResponse statusResponse = tm.Value.Status(request);
                        // TODO: primary???
                        if (statusResponse.Status)
                            Console.WriteLine($"Status: Transaction Manager with id ({tm.Key}) is alive!");
                    }
                    catch (Grpc.Core.RpcException e)
                    {
                        Console.WriteLine(e.Status);
                    }

                    return Task.CompletedTask;
                });

                tasks.Add(t);
            }
            Task.WaitAll(tasks.ToArray());
            return true;
        }

        static void TransactionRequest(string[] command)
        {
            if (command.Length == 3)
            {
                string[] reads = command[1].Split(',', StringSplitOptions.RemoveEmptyEntries);
                string[] writes = command[2].Split(',', StringSplitOptions.RemoveEmptyEntries);

                foreach (string read in reads)
                {
                    Console.WriteLine("DADINT: [" + read + "]");
                }
                foreach (string write in writes)
                {
                    try
                    {
                        string[] writePair = write.Split(':', StringSplitOptions.RemoveEmptyEntries); // TODO??
                        if (writePair.Length != 2)
                        {
                            Console.WriteLine("Invalid write pair provided for transaction request.");
                            return;
                        }
                        int.Parse(writePair[1]);
                        Console.WriteLine("DADINT: [" + writePair[0] + ", " + writePair[1] + "]");
                    }
                    catch (FormatException)
                    {
                        Console.WriteLine("Invalid write pair provided for transaction request.");
                    }
                }
            }
            else { Console.WriteLine("Invalid number of arguments provided for transaction request."); }
        }

        static bool HandleCommand(string command, TransactionManagers transactionManagers)
        {
            string[] commandArgs = command.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            if (command.Length == 0)
            {
                Console.WriteLine("No command provided.");
                return true;
            }

            switch (commandArgs[0].ToLower())
            {
                case "w":
                    Console.WriteLine("Client set to wait...");
                    Wait(commandArgs);
                    break;
                case "t":
                    Console.WriteLine("Processing transaction request...");
                    TransactionRequest(commandArgs);
                    break;
                case "s":
                    Console.WriteLine("Sending status request...");
                    _ = Status();
                    break;
                case "q":
                    Console.WriteLine("Closing client...");
                    return false;
                default:
                    Console.WriteLine("Invalid command.");
                    break;
            }
            return true;
        }

        static void Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Command Line Arguments
            if (args.Length < 2)
            {
                Console.WriteLine("You need to pass at least 2 arguments, processId and scriptName");
                return;
            }
            string processId = args[0];
            string scriptName = args[1]; // TODO: place elsewhere?? launcher??
            bool debug = args.Length > 2 && args[2].Equals("debug");

            Console.WriteLine($"TKVClient with id ({processId}) starting...");

            TKVConfig config;
            try { config = Common.ReadConfig(); }
            catch (Exception)
            {
                Console.WriteLine("Error reading config file.");
                return;
            }
            // TODO: process config file
            (int slotDuration, TimeSpan startTime) = config.SlotDetails;

            // Process data from config file
            transactionManagers = config.TransactionManagers.ToDictionary(key => key.Id, value =>
            {
                GrpcChannel channel = GrpcChannel.ForAddress(value.Url);
                return new Client_TransactionManagerService.Client_TransactionManagerServiceClient(channel);
            });

            // Read client scripts
            string baseDirectory = Common.GetSolutionDir();
            string scriptFilePath = Path.Join(baseDirectory, "TKVClient", "Scripts", scriptName + ".txt");
            Console.WriteLine("Using script (" + scriptFilePath + ") for TKVClient.");

            string[] commands;
            try { commands = File.ReadAllLines(scriptFilePath); }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Script file not found.");
                return;
            }

            // Wait for slots to start
            if (DateTime.Now.TimeOfDay < startTime)
            {
                System.Threading.Thread.Sleep(startTime - DateTime.Now.TimeOfDay);
            }

            int clientTimestamp = 0;

            foreach (string command in commands) { HandleCommand(command, transactionManagers); }

            Console.WriteLine("Press q to exit.");
            while (Console.ReadKey().Key != ConsoleKey.Q) { };
        }
    }
}