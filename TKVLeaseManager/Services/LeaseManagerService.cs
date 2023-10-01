using System.Collections.Concurrent;
using TKVLeaseManager.Domain;
using TransactionManagerLeaseManagerServiceProto;
using LeaseManagerLeaseManagerServiceProto;
using Utilities;

namespace TKVLeaseManager.Services
{
    public class LeaseManagerService
    {
        // Config file variables
        private int _processId;
        ////private readonly List<bool> _processFrozenPerSlot;
        private readonly Dictionary<string, Paxos.PaxosClient> _leaseManagerHosts;
        ////private readonly List<Dictionary<string, List<String>>> _processesSuspectedPerSlot;

        // Changing variables
        ////private bool _isFrozen;
        private int _currentSlot;
        private readonly List<ProcessState> _statePerSlot;
        private readonly ConcurrentDictionary<int, SlotData> _slots;

        public LeaseManagerService(
            string processId,
            ////List<bool> processFrozenPerSlot,
            ////List<Dictionary<string, List<String>>> processesSuspectedPerSlot,
            List<ProcessState> statePerSlot,
            Dictionary<string, Paxos.PaxosClient> leaseManagerHosts
            )
        {
            _processId = processId[^1];
            _leaseManagerHosts = leaseManagerHosts;
            ////_processFrozenPerSlot = processFrozenPerSlot;
            ////_processesSuspectedPerSlot = processesSuspectedPerSlot;

            _currentSlot = 0;
            ////_isFrozen = false;

            _statePerSlot = statePerSlot;
            _slots = new();
            // Initialize slots
            for (var i = 1; i <= statePerSlot.Count; i++)
                _slots.TryAdd(i, new(i));
        }

        /*
         * At the start of every slot this function is called to "prepare the slot".
         * Updates process state (frozen or not).
         * Creates new entry for the slot in the slots dictionary.
         */
        public void PrepareSlot()
        {
            Monitor.Enter(this);
            ////if (_currentSlot >= _processFrozenPerSlot.Count)
            ////{
            ////    Console.WriteLine("Slot duration ended but no more slots to process.");
            ////    return;
            ////}

            Console.WriteLine("Preparing new slot -----------------------");

            // Switch process state
            ////_isFrozen = _processFrozenPerSlot[_currentSlot];
            if (_currentSlot > 0)
                _slots[_currentSlot].IsPaxosRunning = false;
            Monitor.PulseAll(this);
            ////Console.WriteLine($"Process is now {(_isFrozen ? "frozen" : "normal")} for slot {_currentSlot+1}");

            _currentSlot += 1;

            // Every slot increase processId to allow progress when the system configuration changes
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
            ////while (_isFrozen)
            ////{
            ////    Monitor.Wait(this);
            ////}

            var slot = _slots[request.Slot];
  
            if (slot.ReadTimestamp < request.LeaderId[^1])
                slot.ReadTimestamp = request.LeaderId[^1];

            var reply = new PromiseReply
            {
                Slot = request.Slot,
                ReadTimestamp = slot.ReadTimestamp,
                Lease = slot.WrittenValue,
            };

            Console.WriteLine($"({request.Slot})    Received Prepare({request.LeaderId[^1]})");
            Console.WriteLine($"({request.Slot})        Answered Promise({slot.ReadTimestamp},{slot.WrittenValue})");

            Monitor.Exit(this);
            return reply;
        }

        public AcceptedReply AcceptPaxos(AcceptRequest request)
        {
            Monitor.Enter(this);
            ////while (_isFrozen)
            ////{
            ////    Monitor.Wait(this);
            ////}

            var slot = _slots[request.Slot];

            Console.WriteLine($"({request.Slot})    Recevied Accept({request.LeaderId[^1]}, {request.Lease})");

            if (slot.ReadTimestamp == request.LeaderId[^1])
            {
                slot.WriteTimestamp = request.LeaderId[^1];
                slot.WrittenValue = request.Lease;

                // Acceptors send the information to Learners
                SendDecideRequest(slot.Slot, slot.WriteTimestamp, request.Lease);
            }

            Console.WriteLine($"({request.Slot})        Answered Accepted({slot.WriteTimestamp},{slot.WrittenValue})");

            var reply = new AcceptedReply
            {
                Slot = request.Slot,
                WriteTimestamp = slot.WriteTimestamp,
                Lease = slot.WrittenValue,
            };

            Monitor.Exit(this);
            return reply;
        }

        public DecideReply DecidePaxos(DecideRequest request)
        {
            Monitor.Enter(this);
            ////while (_isFrozen)
            ////{
            ////    Monitor.Wait(this);
            ////}

            var slot = _slots[request.Slot];

            Console.WriteLine($"({request.Slot})    Recevied Decide({request.WriteTimestamp},{request.Lease})");

            // Learners keep track of all decided values to check for a majority
            slot.DecidedReceived.Add((request.WriteTimestamp, request.Lease));

            var majority = _leaseManagerHosts.Count / 2 + 1;

            // Create a dictionary to count the number of times a request appears
            var receivedRequests = new Dictionary<(int, Lease), int>();
            foreach (var entry in slot.DecidedReceived)
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
                slot.DecidedValue = requestFrequency.Key.Item2;
                slot.IsPaxosRunning = false;
                Monitor.PulseAll(this);
            }

            Console.WriteLine($"({request.Slot})        Answered Decided()");
            Monitor.Exit(this);
            return new()
            {
            };
        }

        /*
        * Paxos Service (Client) Implementation
        * Communication between leaseManager and leaseManager
        */

        public List<PromiseReply> SendPrepareRequest(int slot, string leaderId)
        {
            var prepareRequest = new PrepareRequest
            {
                Slot = slot,
                LeaderId = leaderId
            };

            Console.WriteLine($"({slot}) Sending Prepare({leaderId})");

            List<PromiseReply> promiseResponses = new();

            List<Task> tasks = new();
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

        public List<AcceptedReply> SendAcceptRequest(int slot, string leaderId, Lease lease)
        {
            var acceptRequest = new AcceptRequest
            {
                Slot = slot,
                LeaderId = leaderId,
                Lease = lease
            };
            
            Console.WriteLine($"({slot}) Sending Accept({leaderId},{lease})");

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

        public void SendDecideRequest(int slot, int writeTimestamp, Lease lease)
        {

            var decideRequest = new DecideRequest
            {
                Slot = slot,
                WriteTimestamp = writeTimestamp,
                Lease = lease
            };

            Console.WriteLine($"({slot}) Sending Decide({writeTimestamp},{lease})");

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

        public bool WaitForPaxos(SlotData slot, LeaseRequest request)
        {
            var success = true;
            while (slot.IsPaxosRunning)
            {
                Monitor.Wait(this);

                // Slot ended without reaching consensus
                // Do paxos again with another configuration
                if (_currentSlot <= slot.Slot || !slot.DecidedValue.Equals(new() { Id = "-1", Permissions = {  }})) continue;
                Console.WriteLine($"Slot {slot.Slot} ended without consensus, starting a new paxos slot in slot {_currentSlot}.");
                success = false;
                break;
            }
            return success;
        }

        public bool DoPaxosSlot(LeaseRequest request)
        {
            //Monitor.Enter(this);
            
            var slot = _slots[request.Slot];

            // If paxos isn't running and a value hasn't been decided, start paxos
            if (!slot.IsPaxosRunning && slot.DecidedValue.Equals(new() { Id = "-1", Permissions = {  }}))
            {   
                slot.IsPaxosRunning = true;
            }
            else
            {
                return WaitForPaxos(slot, request);
            }

            Console.WriteLine($"Starting Paxos slot in slot {_currentSlot} for slot {request.Slot}");

            // Select new leader
            ////var processesSuspected = _processesSuspectedPerSlot[_currentSlot - 1];
            var leader = int.MaxValue;
            ////foreach (var process in processesSuspected)
            ////{
            ////    // leaseManager process that is not suspected and has the lowest id
            ////    if (!process.Value && process.Key < leader && _leaseManagerHosts.ContainsKey(process.Key.ToString()))
            ////        leader = process.Key;
            ////}

            Console.WriteLine($"Paxos leader is {leader} in slot {_currentSlot} for slot {request.Slot}");

            // Save processId for current paxos slot
            // Otherwise it might change in the middle of paxos if a new slot begins
            var leaderCurrentId = _processId.ToString();
            
            // 'leader' comes from config, doesn't account for increase in processId
            ////if (_processId % _leaseManagerHosts.Count != leader)
            ////{
            ////    return WaitForPaxos(slot, request);
            ////}

            Monitor.Exit(this);
            // Send prepare to all acceptors
            List<PromiseReply> promiseResponses = SendPrepareRequest(request.Slot, leaderCurrentId);
            
            Monitor.Enter(this);
            // Stop being leader if there is a more recent one
            //get the last char of _processId
            foreach (var response in promiseResponses)
            {
                if (response.ReadTimestamp > _processId)
                    return WaitForPaxos(slot, request);
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
            if (valueToPropose.Equals(new() { Id = "-1", Permissions = { } }))
                valueToPropose = request.Lease;

            Monitor.Exit(this);
            // Send accept to all acceptors which will send decide to all learners
            SendAcceptRequest(request.Slot, leaderCurrentId, valueToPropose);

            Monitor.Enter(this);
            // Wait for learners to decide
            return WaitForPaxos(slot, request);
        }

        public LeaseResponse LeaseRequest(LeaseRequest request)
        {
            Monitor.Enter(this);
            ////while (_isFrozen)
            ////{
            ////    Monitor.Wait(this);
            ////}

            var slot = _slots[request.Slot];
        
            Console.WriteLine($"Lease request with value {request.Lease} in slot {request.Slot}");

            while (!DoPaxosSlot(request))
            {   
            }
            
            Monitor.Exit(this);

            Console.WriteLine($"Compare and swap replied with value {slot.DecidedValue} for slot {request.Slot}");

            return new()
            {
                Slot = request.Slot,
                Status = true
            };
        }
    }
}