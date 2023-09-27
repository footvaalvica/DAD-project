using System.Collections.Concurrent;
using LeaseManagerLeaseManagerServiceProto;
using TKVLeaseManager.Domain;
using TransactionManagerLeaseManagerServiceProto;

namespace TKVLeaseManager.Services
{
    public class LeaseManagerService
    {
        // Config file variables
        private int _processId;
        private readonly List<bool> _processFrozenPerInstance;
        private readonly Dictionary<int, Paxos.PaxosClient> _leaseManagerHosts;
        private readonly List<Dictionary<int, bool>> _processesSuspectedPerInstance;

        // Changing variables
        private bool _isFrozen;
        private int _currentInstance;
        private readonly ConcurrentDictionary<int, InstanceData> _instances;

        public LeaseManagerService(
            int processId,
            List<bool> processFrozenPerInstance,
            List<Dictionary<int, bool>> processesSuspectedPerInstance,
            Dictionary<int, Paxos.PaxosClient> leaseManagerHosts
            )
        {
            _processId = processId;
            _leaseManagerHosts = leaseManagerHosts;
            _processFrozenPerInstance = processFrozenPerInstance;
            _processesSuspectedPerInstance = processesSuspectedPerInstance;

            _currentInstance = 0;
            _isFrozen = false;

            _instances = new ConcurrentDictionary<int, InstanceData>();
            // Initialize instances
            for (var i = 1; i <= processFrozenPerInstance.Count; i++)
                _instances.TryAdd(i, new InstanceData(i));
        }

        /*
         * At the start of every instance this function is called to "prepare the instance".
         * Updates process state (frozen or not).
         * Creates new entry for the instance in the instances dictionary.
         */
        public void PrepareInstance()
        {
            Monitor.Enter(this);
            if (_currentInstance >= _processFrozenPerInstance.Count)
            {
                Console.WriteLine("Instance duration ended but no more instances to process.");
                return;
            }

            Console.WriteLine("Preparing new instance -----------------------");

            // Switch process state
            _isFrozen = _processFrozenPerInstance[_currentInstance];
            if (_currentInstance > 0)
                _instances[_currentInstance].IsPaxosRunning = false;
            Monitor.PulseAll(this);
            Console.WriteLine($"Process is now {(_isFrozen ? "frozen" : "normal")} for instance {_currentInstance+1}");

            _currentInstance += 1;

            // Every instance increase processId to allow progress when the system configuration changes
            _processId += _leaseManagerHosts.Count;

            Console.WriteLine("Ending preparation -----------------------");
            Monitor.Exit(this);
        }

        /*
        * Paxos Service (Server) Implementation
        * Communication between leaseManager and leaseManager
        */

        public PromiseReply PreparePaxos(PrepareRequest request)
        {
            Monitor.Enter(this);
            while (_isFrozen)
            {
                Monitor.Wait(this);
            }

            var instance = _instances[request.Instance];
  
            if (instance.ReadTimestamp < request.LeaderId)
                instance.ReadTimestamp = request.LeaderId;

            var reply = new PromiseReply
            {
                Instance = request.Instance,
                ReadTimestamp = instance.ReadTimestamp,
                Lease = instance.WrittenValue,
            };

            Console.WriteLine($"({request.Instance})    Received Prepare({request.LeaderId})");
            Console.WriteLine($"({request.Instance})        Answered Promise({instance.ReadTimestamp},{instance.WrittenValue})");

            Monitor.Exit(this);
            return reply;
        }

        public AcceptedReply AcceptPaxos(AcceptRequest request)
        {
            Monitor.Enter(this);
            while (_isFrozen)
            {
                Monitor.Wait(this);
            }

            var instance = _instances[request.Instance];

            Console.WriteLine($"({request.Instance})    Recevied Accept({request.LeaderId}, {request.Lease})");

            if (instance.ReadTimestamp == request.LeaderId)
            {
                instance.WriteTimestamp = request.LeaderId;
                instance.WrittenValue = request.Lease;

                // Acceptors send the information to Learners
                SendDecideRequest(instance.Instance, instance.WriteTimestamp, request.Lease);
            }

            Console.WriteLine($"({request.Instance})        Answered Accepted({instance.WriteTimestamp},{instance.WrittenValue})");

            var reply = new AcceptedReply
            {
                Instance = request.Instance,
                WriteTimestamp = instance.WriteTimestamp,
                Lease = instance.WrittenValue,
            };

            Monitor.Exit(this);
            return reply;
        }

        public DecideReply DecidePaxos(DecideRequest request)
        {
            Monitor.Enter(this);
            while (_isFrozen)
            {
                Monitor.Wait(this);
            }

            var instance = _instances[request.Instance];

            Console.WriteLine($"({request.Instance})    Recevied Decide({request.WriteTimestamp},{request.Lease})");

            // Learners keep track of all decided values to check for a majority
            instance.DecidedReceived.Add((request.WriteTimestamp, request.Lease));

            var majority = _leaseManagerHosts.Count / 2 + 1;

            // Create a dictionary to count the number of times a request appears
            var receivedRequests = new Dictionary<(int, Lease), int>();
            foreach (var entry in instance.DecidedReceived)
            {
                if (receivedRequests.ContainsKey(entry))
                    receivedRequests[entry]++;
                else
                    receivedRequests.Add(entry, 1);
            }
            
            // If a request appears more times than the majority value, it's the decided value
            foreach (var requestFrequency in receivedRequests)
            {
                if (requestFrequency.Value < majority) continue;
                instance.DecidedValue = requestFrequency.Key.Item2;
                instance.IsPaxosRunning = false;
                Monitor.PulseAll(this);
            }

            Console.WriteLine($"({request.Instance})        Answered Decided()");
            Monitor.Exit(this);
            return new DecideReply
            {
            };
        }

        /*
        * Paxos Service (Client) Implementation
        * Communication between leaseManager and leaseManager
        */

        public List<PromiseReply> SendPrepareRequest(int instance, int leaderId)
        {
            var prepareRequest = new PrepareRequest
            {
                Instance = instance,
                LeaderId = leaderId
            };

            Console.WriteLine($"({instance}) Sending Prepare({leaderId})");

            List<PromiseReply> promiseResponses = new List<PromiseReply>();

            List<Task> tasks = new List<Task>();
            foreach (var host in _leaseManagerHosts)
            {
                var t = Task.Run(() =>
                {
                    try
                    {
                        var promiseReply = host.Value.Prepare(prepareRequest);
                        promiseResponses.Add(promiseReply);
                    }
                    catch (Grpc.Core.RpcException e)
                    {
                        Console.WriteLine(e.Status);
                    }
                    return Task.CompletedTask;
                });
                tasks.Add(t);
            }

            for (var i = 0; i < _leaseManagerHosts.Count / 2 + 1; i++)
                tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));

            return promiseResponses;
        }

        public List<AcceptedReply> SendAcceptRequest(int instance, int leaderId, Lease lease)
        {
            var acceptRequest = new AcceptRequest
            {
                Instance = instance,
                LeaderId = leaderId,
                Lease = lease
            };
            
            Console.WriteLine($"({instance}) Sending Accept({leaderId},{lease})");

            var acceptResponses = new List<AcceptedReply>();

            var tasks = _leaseManagerHosts.Select(host => Task.Run(() =>
                {
                    try
                    {
                        var acceptedReply = host.Value.Accept(acceptRequest);
                        acceptResponses.Add(acceptedReply);
                    }
                    catch (Grpc.Core.RpcException e)
                    {
                        Console.WriteLine(e.Status);
                    }

                    return Task.CompletedTask;
                }))
                .ToList();

            // Wait for a majority of responses
            for (var i = 0; i < _leaseManagerHosts.Count / 2 + 1; i++)
                tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));

            return acceptResponses;
        }

        public void SendDecideRequest(int instance, int writeTimestamp, Lease lease)
        {

            var decideRequest = new DecideRequest
            {
                Instance = instance,
                WriteTimestamp = writeTimestamp,
                Lease = lease
            };

            Console.WriteLine($"({instance}) Sending Decide({writeTimestamp},{lease})");

            foreach (var t in _leaseManagerHosts.Select(host => Task.Run(() =>
                     {
                         try
                         {
                             var decideReply = host.Value.Decide(decideRequest);
                         }
                         catch (Grpc.Core.RpcException e)
                         {
                             Console.WriteLine(e.Status);
                         }
                         return Task.CompletedTask;
                     })))
            {
            }

            // Don't need to wait for majority
        }

        /*
         * Compare And Swap Service (Server) Implementation
         * Communication between Bank and leaseManager
         */

        public bool WaitForPaxos(InstanceData instance, LeaseRequest request)
        {
            var success = true;
            while (instance.IsPaxosRunning)
            {
                Monitor.Wait(this);

                // Instance ended without reaching consensus
                // Do paxos again with another configuration
                if (_currentInstance <= instance.Instance || !instance.DecidedValue.Equals(new Lease() { Id = "-1", Permissions = {  }})) continue;
                Console.WriteLine($"Instance {instance.Instance} ended without consensus, starting a new paxos instance in instance {_currentInstance}.");
                success = false;
                break;
            }
            return success;
        }

        public bool DoPaxosInstance(LeaseRequest request)
        {
            //Monitor.Enter(this);
            
            var instance = _instances[request.Instance];

            // If paxos isn't running and a value hasn't been decided, start paxos
            if (!instance.IsPaxosRunning && instance.DecidedValue.Equals(new Lease() { Id = "-1", Permissions = {  }}))
            {   
                instance.IsPaxosRunning = true;
            }
            else
            {
                return WaitForPaxos(instance, request);
            }

            Console.WriteLine($"Starting Paxos instance in instance {_currentInstance} for instance {request.Instance}");

            // Select new leader
            var processesSuspected = _processesSuspectedPerInstance[_currentInstance - 1];
            var leader = int.MaxValue;
            foreach (var process in processesSuspected)
            {
                // leaseManager process that is not suspected and has the lowest id
                if (!process.Value && process.Key < leader && _leaseManagerHosts.ContainsKey(process.Key))
                    leader = process.Key;
            }
            
            Console.WriteLine($"Paxos leader is {leader} in instance {_currentInstance} for instance {request.Instance}");

            // Save processId for current paxos instance
            // Otherwise it might change in the middle of paxos if a new instance begins
            var leaderCurrentId = _processId;
            
            // 'leader' comes from config, doesn't account for increase in processId
            if (_processId % _leaseManagerHosts.Count != leader)
            {
                return WaitForPaxos(instance, request);
            }

            Monitor.Exit(this);
            // Send prepare to all acceptors
            List<PromiseReply> promiseResponses = SendPrepareRequest(request.Instance, leaderCurrentId);
            
            Monitor.Enter(this);
            // Stop being leader if there is a more recent one
            foreach (var response in promiseResponses)
            {
                if (response.ReadTimestamp > _processId)
                    return WaitForPaxos(instance, request);
            }

            // Get values from promises
            var mostRecent = -1;
            var valueToPropose = new Lease() { Id = "-1", Permissions = {  }};
            foreach (var response in promiseResponses)
            {
                if (response.ReadTimestamp > mostRecent)
                {
                    mostRecent = response.ReadTimestamp;
                    valueToPropose = response.Lease;
                }
            }

            // If acceptors have no value, send own value
            if (valueToPropose.Equals(new Lease() { Id = "-1", Permissions = { } }))
                valueToPropose = new Lease() { Id = request.Id, Permissions = { request.Permissions } };

            Monitor.Exit(this);
            // Send accept to all acceptors which will send decide to all learners
            SendAcceptRequest(request.Instance, leaderCurrentId, valueToPropose);

            Monitor.Enter(this);
            // Wait for learners to decide
            return WaitForPaxos(instance, request);
        }

        public LeaseResponse LeaseRequest(LeaseRequest request)
        {
            Monitor.Enter(this);
            while (_isFrozen)
            {
                Monitor.Wait(this);
            }

            var instance = _instances[request.Instance];
        
            Console.WriteLine($"Compare and swap request with value {request.Id} and {request.Permissions} in instance {request.Instance}");

            while (!DoPaxosInstance(request))
            {   
            }
            
            Monitor.Exit(this);

            Console.WriteLine($"Compare and swap replied with value {instance.DecidedValue} for instance {request.Instance}");

            return new LeaseResponse
            {
                Instance = request.Instance,
                Status = true
            };
        }
    }
}