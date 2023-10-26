using Grpc.Net.Client;
using Utilities;
using ClientTransactionManagerProto;
using ClientLeaseManagerProto;
using System.Text.RegularExpressions;
using Grpc.Core;

namespace TKVClient
{
    using TransactionManagers = Dictionary<string, Client_TransactionManagerService.Client_TransactionManagerServiceClient>;
    internal class Program
    {
        static TransactionManagers? transactionManagers = null;
        static TKVConfig config;
        static void Wait(string[] command)
        {
            try
            {
                if (command.Length == 2)
                {
                    Console.WriteLine("Waiting for " + command[1] + " milliseconds...");

                    int millisecondsToWait = int.Parse(command[1]);
                    DateTime startTime = DateTime.Now;
                    // Wait loop
                    while ((DateTime.Now - startTime).TotalMilliseconds < millisecondsToWait)
                    {
                        // Check for key press
                        if (Console.KeyAvailable)
                        {
                            Console.WriteLine("Key pressed. Wait interrupted.");
                            return;
                        }

                        //Sleep for a shorter interval to make the check more responsive
                        Thread.Sleep(100);
                    }
                    Console.WriteLine("Wait completed.");
                }
                else
                {
                    Console.WriteLine("No time amount provided for wait.");
                }
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

        static List<DADInt> TxSubmit(string id, List<String> reads, List<DADInt> writes)
        {
            TransactionRequest request = new TransactionRequest { Id = id };
            request.Reads.AddRange(reads);
            request.Writes.AddRange(writes);

            int indexTM = config.Clients.IndexOf(id) % transactionManagers.Count;
            string tm = config.TransactionManagers[indexTM].Id;

            // send request to transaction manager
            bool transactionSent = false;
            while (!transactionSent)
            {
                try
                {
                    TransactionResponse response = transactionManagers[tm].TxSubmit(request);
                    if (response.Response != null)
                    {
                        Console.WriteLine("Transaction successful!");
                        transactionSent = true;
                        return response.Response.Select(read => new DADInt { Key = read.Key, Value = read.Value }).ToList();
                    }
                    else
                    {
                        Console.WriteLine("Transaction failed!");
                    }
                }
                catch (Grpc.Core.RpcException e)
                {
                    // if TM is crashed, then submit transaction request to the next TM
                    Console.WriteLine(e.Status);
                    indexTM = (++indexTM) % transactionManagers.Count;
                    tm = config.TransactionManagers[indexTM].Id;
                    //Console.WriteLine($"TM is now {tm}");
                }
            }   

            return new List<DADInt>();
        }

        static void TransactionRequest(string[] command, string processId)
        {
            if (command.Length == 3)
            {
                // Remove parenthesis and etc
                string[] reads = command[1].Substring(1, command[1].Length - 2)
                    .Split(',', StringSplitOptions.RemoveEmptyEntries);
                reads = reads.Select(read => read.Trim('"')).ToArray();

                foreach (string read in reads)
                {
                    //Console.WriteLine("DADINT: [" + read + "]");
                }

                Regex rg = new Regex(@"<""([^""]+)"",(\d+)>");
                MatchCollection matched = rg.Matches(command[2]);

                List<DADInt> writesList = new List<DADInt>();
                foreach (Match match in matched)
                {
                    if (match.Groups.Count % 2 != 1)
                    {
                        Console.WriteLine("Invalid transaction request.");
                        continue;
                    }
                    for (int i = 1; i < match.Groups.Count; i += 2)
                    {
                        string key = match.Groups[i].Value;
                        string number = match.Groups[i + 1].Value;
                        try
                        {
                            //Console.WriteLine("DADINT: [" + key + ", " + number + "]");

                            writesList.Add(new DADInt { Key = key, Value = int.Parse(number) });

                        }
                        catch (FormatException)
                        {
                            Console.WriteLine("Invalid write pair provided for transaction request.");
                        }
                    }
                }

                List<DADInt> dadintsRead = TxSubmit(processId, reads.ToList(), writesList);
                foreach (DADInt dadint in dadintsRead)
                {
                    Console.WriteLine("DADINT: [" + dadint.Key + ", " + dadint.Value + "]");
                }
            }
            else { Console.WriteLine("Invalid number of arguments provided for transaction request."); }
        }

        static bool HandleCommand(string command, string processId, TransactionManagers transactionManagers)
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
                    TransactionRequest(commandArgs, processId);
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
            string scriptName = args[1];
            TimeSpan startTime = TimeSpan.Zero;
            if (args.Length > 2)
            {
                startTime = TimeSpan.Parse(args[2]);
            }
                
            Console.WriteLine($"TKVClient with id ({processId}) starting...");

            try { config = Common.ReadConfig(); }
            catch (Exception)
            {
                Console.WriteLine("Error reading config file.");
                return;
            }

            (int slotDuration, _) = config.SlotDetails;

            // Process data from config file
            transactionManagers = config.TransactionManagers.ToDictionary(key => key.Id, value =>
            {
                GrpcChannel channel = GrpcChannel.ForAddress(value.Url);
                return new Client_TransactionManagerService.Client_TransactionManagerServiceClient(channel);
            });

            // Read client scripts
            string baseDirectory = Common.GetSolutionDirectory();
            string scriptFilePath = Path.Join(baseDirectory, "TKVClient", "Scripts", scriptName + ".txt");
            Console.WriteLine("Using script (" + scriptFilePath + ") for TKVClient.");

            string[] commands;
            try { commands = File.ReadAllLines(scriptFilePath); }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Script file not found.");
                return;
            }

            if (startTime != TimeSpan.Zero)
            {
                // Wait for slots to start
                if (DateTime.Now.TimeOfDay < startTime)
                {
                    System.Threading.Thread.Sleep(startTime - DateTime.Now.TimeOfDay);
                }
            }

            // handle commands from script file while client is running

            Console.WriteLine("Press any key to exit.");

            while (!Console.KeyAvailable)
            {
                foreach (string command in commands)
                {
                    if (!command[0].Equals('#'))
                    {
                        Console.WriteLine("Command: " + command);
                        HandleCommand(command, processId, transactionManagers);
                    }                  
                }

                // Read commands from the file and continue the loop
                commands = File.ReadAllLines(scriptFilePath);
            }

            // Optional: Read the key to clear the input buffer and exit
            Console.ReadKey(intercept: true);

        }
    }
}