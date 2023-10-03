using System.Collections.Concurrent;
using TKVLeaseManager.Domain;
using TransactionManagerLeaseManagerServiceProto;
using LeaseManagerLeaseManagerServiceProto;
using Utilities;
using System.Runtime.Serialization.Formatters;

namespace TKVLeaseManager.Services
{
    public class LeaseManagerService
    {
        // Config file variables
        private int _processId;
        private string _processName;
        ////private readonly List<bool> _processFrozenPerSlot;
        private readonly Dictionary<string, Paxos.PaxosClient> _leaseManagerHosts;
        ////private readonly List<Dictionary<string, List<String>>> _processesSuspectedPerSlot;

        // Changing variables
        ////private bool _isFrozen;
        private int _currentSlot;
        private readonly List<List<ProcessState>> _statePerSlot;
        private readonly List<string> _processBook;
        // TODO change this to a list
        private readonly ConcurrentDictionary<int, SlotData> _slots;
        private string? _leader = null; // TODO

        public LeaseManagerService(
            int processId,
            string processName,
            ////List<bool> processFrozenPerSlot,
            ////List<Dictionary<string, List<String>>> processesSuspectedPerSlot,
            List<string> processBook,
            List<List<ProcessState>> statePerSlot,
            Dictionary<string, Paxos.PaxosClient> leaseManagerHosts
            )
        {
            _processName = processName;
            _processId = processId;
            _leaseManagerHosts = leaseManagerHosts;
            ////_processFrozenPerSlot = processFrozenPerSlot;
            ////_processesSuspectedPerSlot = processesSuspectedPerSlot;

            _currentSlot = 0;
            ////_isFrozen = false;

            _processBook = processBook;
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

            // Every slot increase processId to allow progress when the system configuration changes // TODO????
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

            Console.WriteLine($"({request.Slot})    Received Prepare({request.LeaderId} - {_processBook[request.LeaderId % _leaseManagerHosts.Count]})");

            var slot = _slots[request.Slot];
  
            if (slot.ReadTimestamp < request.LeaderId)
                slot.ReadTimestamp = request.LeaderId;

            var reply = new PromiseReply
            {
                Slot = request.Slot,
                ReadTimestamp = slot.ReadTimestamp,
                Lease = slot.WrittenValue,
            };

            Console.WriteLine($"({request.Slot})    Received Prepare({request.LeaderId} - {_processBook[request.LeaderId % _leaseManagerHosts.Count]})");
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

            Console.WriteLine($"({request.Slot})    Received Accept({request.LeaderId} - {_processBook[request.LeaderId % _leaseManagerHosts.Count]}, {request.Lease})");

            if (slot.ReadTimestamp == request.LeaderId)
            {
                slot.WriteTimestamp = request.LeaderId;
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

            Console.WriteLine($"({request.Slot})    Received Decide({request.WriteTimestamp},{request.Lease})");

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

        public List<PromiseReply> SendPrepareRequest(int slot, int leaderId)
        {
            var prepareRequest = new PrepareRequest
            {
                Slot = slot,
                LeaderId = leaderId
            };

            Console.WriteLine($"({slot}) Sending Prepare({leaderId % _leaseManagerHosts.Count})");

            List<PromiseReply> promiseResponses = new();

            List<Task> tasks = new();
            foreach (var host in _leaseManagerHosts)
            {
                if (host.Key == _processBook[leaderId % _leaseManagerHosts.Count]) continue; // TODO?
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
                Console.WriteLine("Sent prepare request");
            }

            for (var i = 0; i < _leaseManagerHosts.Count / 2 + 1; i++)
                tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));

            return promiseResponses;
        }

        public List<AcceptedReply> SendAcceptRequest(int slot, int leaderId, Lease lease)
        {
            var acceptRequest = new AcceptRequest
            {
                Slot = slot,
                LeaderId = leaderId,
                Lease = lease
            };
            
            Console.WriteLine($"({slot}) Sending Accept({leaderId % _leaseManagerHosts.Count},{lease})");

            var acceptResponses = new List<AcceptedReply>();

            var tasks = _leaseManagerHosts.Where(host => host.Key != _processBook[leaderId % _leaseManagerHosts.Count])
                .Select(host => Task.Run(() =>
                {
                    Console.WriteLine("Sending accept request");
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

            Console.WriteLine("Waiting for majority accepts");
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

        public bool WaitForPaxos(SlotData slot)
        {
            var success = true;
            Console.WriteLine($"Paxos Running?: {(slot.IsPaxosRunning ? "true" : "false")}");
            while (slot.IsPaxosRunning)
            {
                Monitor.Wait(this);

                // Slot ended without reaching consensus
                // Do paxos again with another configuration
                Console.WriteLine($"Curr.Slot ({_currentSlot}), Slot({slot.Slot}), Equals({(slot.DecidedValue.Equals(new() { Id = "-1", Permissions = { } }) ? "true" : "false")})");
                if (_currentSlot <= slot.Slot && !slot.DecidedValue.Equals(new() { Id = "-1", Permissions = {  }})) continue;
                Console.WriteLine($"Slot {slot.Slot} ended without consensus, starting a new paxos slot in slot {_currentSlot}.");
                success = false;
                break;
            }
            return success;
        }

        public bool DoPaxosSlot(LeaseRequest request)
        {
            Monitor.Enter(this);

            var slot = _slots[_currentSlot];

            // If paxos isn't running and a value hasn't been decided, start paxos
            if (!slot.IsPaxosRunning && slot.DecidedValue.Equals(new() { Id = "-1", Permissions = { } }))
            {
                slot.IsPaxosRunning = true;
            }

            // 1: who's the leader?
            // Suspected (bool) VS Suspects (list of strings)
            // 
            var leader = int.MaxValue;
            for (int i=0; i < _statePerSlot[_currentSlot-1].Count; i++)
            {
                if (_statePerSlot[_currentSlot - 1][i].Crashed == false) // already getting the first lowest id
                {
                    leader = i;
                    break;
                }
            }

            if (leader == int.MaxValue)
            {
                Console.WriteLine("No leader found"); // Should never happen
                return false;
            }

            // 2: is the leader me?
            if (_processId % _leaseManagerHosts.Count != leader)
            {
                Console.WriteLine($"I'm not the leader, I'm process {_processId % _leaseManagerHosts.Count} and the leader is process {leader}");
                return WaitForPaxos(slot);
            }

            Console.WriteLine($"Starting Paxos slot in slot {_currentSlot} for slot {_currentSlot}");

            // Select new leader
            ////var processesSuspected = _processesSuspectedPerSlot[_currentSlot - 1];
            //var leader = int.MaxValue;

            ////foreach (var process in processesSuspected)
            ////{
            ////    // leaseManager process that is not suspected and has the lowest id
            ////    if (!process.Value && process.Key < leader && _leaseManagerHosts.ContainsKey(process.Key.ToString()))
            ////        leader = process.Key;
            ////}

            Console.WriteLine($"Paxos leader is {leader} in slot {_currentSlot}");

            // Save processId for current paxos slot
            // Otherwise it might change in the middle of paxos if a new slot begins
            var leaderCurrentId = _processId;
            
            // 'leader' comes from config, doesn't account for increase in processId
            ////if (_processId % _leaseManagerHosts.Count != leader)
            ////{
            ////    return WaitForPaxos(slot, request);
            ////}

            Monitor.Exit(this);
            // Send prepare to all acceptors
            List<PromiseReply> promiseResponses = SendPrepareRequest(_currentSlot, leaderCurrentId);
            
            Monitor.Enter(this);
            // Stop being leader if there is a more recent one
            //get the last char of _processId
            foreach (var response in promiseResponses)
            {
                if (response.ReadTimestamp > _processId)
                    return WaitForPaxos(slot);
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
            SendAcceptRequest(_currentSlot, leaderCurrentId, valueToPropose);

            Monitor.Enter(this);
            // Wait for learners to decide
            return WaitForPaxos(slot);
        }

        public LeaseResponse LeaseRequest(LeaseRequest request)
        {
            Monitor.Enter(this);
            while (!DoPaxosSlot(request))
            {   
            }

            Monitor.Exit(this);
            return new()
            {
                Slot = _currentSlot,
                Status = true // TODO
            };
        }
    }
}