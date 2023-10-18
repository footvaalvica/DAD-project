using System.Collections.Concurrent;
using TKVLeaseManager.Domain;
using TransactionManagerLeaseManagerServiceProto;
using LeaseManagerLeaseManagerServiceProto;
using Utilities;
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

namespace TKVLeaseManager.Services
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class TupleEqualityComparer<T1, T2> : IEqualityComparer<(T1, T2)>
    {
        public bool Equals((T1, T2) x, (T1, T2) y)
        {
            return EqualityComparer<T1>.Default.Equals(x.Item1, y.Item1) &&
                   Enumerable.SequenceEqual((IEnumerable<Lease>)x.Item2, (IEnumerable<Lease>)y.Item2); // Compare IEnumerable<Lease> based on its elements
        }

        public int GetHashCode((T1, T2) obj)
        {
            unchecked
            {
                int hash = 17;
                hash = hash * 23 + obj.Item1.GetHashCode();
                foreach (var item in (IEnumerable<Lease>)obj.Item2)
                {
                    hash = hash * 23 + item.GetHashCode();
                }
                return hash;
            }
        }
    }

    public class LeaseManagerService
    {
        // Config file variables
        private int _processId;
        private string _processName;
        private readonly Dictionary<string, Paxos.PaxosClient> _leaseManagerHosts;

        // Changing variables
        private bool _isCrashed;
        private int _currentSlot;
        private readonly List<List<ProcessState>> _statePerSlot;
        private readonly List<string> _processBook;
        private readonly ConcurrentDictionary<int, SlotData> _slots;
        private string? _leader = null; // TO CHECK AFTER CHECKPOINT
        private List<LeaseRequest> _bufferLeaseRequests = new();

        public LeaseManagerService(
            int processId,
            string processName,
            List<string> processBook,
            List<List<ProcessState>> statePerSlot,
            Dictionary<string, Paxos.PaxosClient> leaseManagerHosts
            )
        {
            _processName = processName;
            _processId = processId;
            _leaseManagerHosts = leaseManagerHosts;
            _currentSlot = 0;
            _isCrashed = false;

            _processBook = processBook;
            _statePerSlot = statePerSlot;
            _slots = new ConcurrentDictionary<int, SlotData>();
            // Initialize slots
            for (var i = 1; i <= statePerSlot.Count; i++)
                _slots.TryAdd(i, new SlotData(i));
        }

        /*
         * At the start of every slot this function is called to "prepare the slot".
         * Updates process state (frozen or not).
         * Creates new entry for the slot in the slots dictionary.
         */
        public void PrepareSlot()
        {
            Monitor.Enter(this);
            if (_currentSlot >= _statePerSlot.Count)
            {
                Console.WriteLine("Slot duration ended but no more slots to process.");
                return;
            }
            
            Console.WriteLine("Preparing new slot -----------------------");

            //Console.WriteLine($"Have ({_bufferLeaseRequests.Count}) requests to process for this slot");

            // Switch process state
            _isCrashed = _statePerSlot[_currentSlot][_processId % _leaseManagerHosts.Count].Crashed;
            Console.WriteLine($"Process is now {(_isCrashed ? "crashed" : "normal")} for slot {_currentSlot}");

            if (_currentSlot > 0)
            {
                _slots[_currentSlot].IsPaxosRunning = false;
            }

            Monitor.PulseAll(this);

            Monitor.Exit(this);
            DoPaxosSlot();
            Monitor.Enter(this);

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
            while (_isCrashed)
            {
                Monitor.Wait(this);
            }

            //Console.WriteLine($"({request.Slot})    Received Prepare({request.LeaderId} - {_processBook[request.LeaderId % _leaseManagerHosts.Count]})");

            var slot = _slots[request.Slot];

            if (slot.ReadTimestamp < request.LeaderId)
                slot.ReadTimestamp = request.LeaderId;

            var reply = new PromiseReply
            {
                Slot = request.Slot,
                ReadTimestamp = slot.ReadTimestamp,
            };
            reply.Leases.AddRange(slot.WrittenValues);

            //Console.WriteLine($"({request.Slot})    Received Prepare({request.LeaderId} - {_processBook[request.LeaderId % _leaseManagerHosts.Count]})");
            //Console.WriteLine($"({request.Slot})        Answered Promise({slot.ReadTimestamp},{slot.WrittenValues})");

            Monitor.Exit(this);
            return reply;
        }

        public AcceptedReply AcceptPaxos(AcceptRequest request)
        {
            Monitor.Enter(this);
            while (_isCrashed)
            {
                Monitor.Wait(this);
            }

            var slot = _slots[request.Slot];

            //Console.WriteLine($"({request.Slot})    Received Accept({request.LeaderId} - {_processBook[request.LeaderId % _leaseManagerHosts.Count]}, {request.Leases})");

            if (slot.ReadTimestamp == request.LeaderId)
            {
                slot.WriteTimestamp = request.LeaderId;
                // Acceptors send the information to Learners
                //Console.WriteLine("going to send");
                Monitor.Exit(this);
                SendDecideRequest(slot.Slot, slot.WriteTimestamp, request.Leases);
                Monitor.Enter(this);
                //Console.WriteLine("sent");
            }
            slot.WrittenValues.AddRange(request.Leases);

            //Console.WriteLine($"({request.Slot})        Answered Accepted({slot.WriteTimestamp},{slot.WrittenValues})");

            var reply = new AcceptedReply
            {
                Slot = request.Slot,
                WriteTimestamp = slot.WriteTimestamp,
            };
            reply.Leases.AddRange(slot.WrittenValues);

            Monitor.Exit(this);
            return reply;
        }

        public DecideReply DecidePaxos(DecideRequest request)
        {
            Monitor.Enter(this);
            while (_isCrashed)
            {
                Monitor.Wait(this);
            }

            var slot = _slots[request.Slot];

            //Console.WriteLine($"({request.Slot})    Received Decide({request.WriteTimestamp},{request.Leases})");

            // Learners keep track of all decided values to check for a majority
            var decidedValue = (request.WriteTimestamp, request.Leases.ToList());
            
            slot.DecidedReceived.Add(decidedValue);

            var majority = _leaseManagerHosts.Count / 2 + 1;

            // Create a dictionary to count the number of times a request appears
            var receivedRequests = new Dictionary<(int, List<Lease>), int>(new TupleEqualityComparer<int, List<Lease>>());
            foreach (var entry in slot.DecidedReceived)
            {
                var key = (entry.Item1, entry.Item2); // Create a tuple with the same structure
                if (receivedRequests.ContainsKey(key))
                {
                    receivedRequests[key]++;
                }
                else
                {
                    receivedRequests.Add(key, 1);
                }
            }

            // If a request appears more times than the majority value, it's the decided value
            foreach (var requestFrequency in receivedRequests)
            {
                //Console.WriteLine("if a request appears more times");
                if (requestFrequency.Value < majority) continue;
                slot.DecidedValues = requestFrequency.Key.Item2;
                slot.IsPaxosRunning = false;
                Monitor.PulseAll(this);
            }

            //Console.WriteLine($"({request.Slot})        Answered Decided()");

            //Console.WriteLine($"Removing requests from buffer");
            foreach (Lease lease in request.Leases)
            {
                // Remove Lease request that contains this lease if the request is in the buffer
                _bufferLeaseRequests = _bufferLeaseRequests.Where(leaseRequest => leaseRequest.Lease.Equals(lease)).ToList(); // TODO - CHECK AFTER CHECKPOINT: not very concurrency friendly
            }
            //Console.WriteLine("Finished removing requests from buffer");

            Monitor.Exit(this);
            return new DecideReply
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

            //Console.WriteLine($"({slot}) Sending Prepare({leaderId % _leaseManagerHosts.Count})");

            List<PromiseReply> promiseResponses = new();

            List<string> badHosts = new();
            for (int i = 0; i < _processBook.Count; i++)
            {
                if (_statePerSlot[slot][i].Crashed)
                    badHosts.Add(_processBook[i]);
            }

            List<Task> tasks = new();
            foreach (var host in _leaseManagerHosts.Where(host => !badHosts.Contains(host.Key)
                && !_statePerSlot[_currentSlot][_processId % _leaseManagerHosts.Count].Suspects.Contains(host.Key)))
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
                //Console.WriteLine("Sent prepare request");
            }

            for (var i = 0; i < _leaseManagerHosts.Count / 2 + 1; i++)
            {
                tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
            }

            Console.WriteLine("Got majority promise responses");

            return promiseResponses;
        }

        public List<AcceptedReply> SendAcceptRequest(int slot, int leaderId, List<Lease> lease)
        {
            var acceptRequest = new AcceptRequest
            {
                Slot = slot,
                LeaderId = leaderId,
            };
            acceptRequest.Leases.AddRange(lease);

            List<string> badHosts = new();
            for (int i = 0; i < _processBook.Count; i++)
            {
                if (_statePerSlot[slot][i].Crashed)
                    badHosts.Add(_processBook[i]);
            }

            //Console.WriteLine($"({slot}) Sending Accept({leaderId % _leaseManagerHosts.Count},{lease})");

            var acceptResponses = new List<AcceptedReply>();

            var tasks = _leaseManagerHosts.Where(host => !badHosts.Contains(host.Key)
                && !_statePerSlot[_currentSlot][_processId % _leaseManagerHosts.Count].Suspects.Contains(host.Key))
                .Select(host => Task.Run(() =>
                {
                    //Console.WriteLine("Sending accept request");
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

            //Console.WriteLine("Waiting for majority accepts");

            // Wait for a majority of responses
            for (var i = 0; i < _leaseManagerHosts.Count / 2 + 1; i++)
            {
                tasks.RemoveAt(Task.WaitAny(tasks.ToArray()));
            }

            Console.WriteLine("Got majority accepts!");
            return acceptResponses;
        }

        public void SendDecideRequest(int slot, int writeTimestamp, RepeatedField<Lease> lease)
        {
            var decideRequest = new DecideRequest
            {
                Slot = slot,
                WriteTimestamp = writeTimestamp,
            };
            decideRequest.Leases.AddRange(lease);

            List<string> badHosts = new();
            for (int i=0; i<_processBook.Count; i++)
            {
                if (_statePerSlot[slot][i].Crashed)
                    badHosts.Add(_processBook[i]);
            }

            //Console.WriteLine($"({slot}) Sending Decide({writeTimestamp},{lease})");
            foreach (var t in _leaseManagerHosts.Where(host => host.Key != _processName && !badHosts.Contains(host.Key)
              && !_statePerSlot[_currentSlot][_processId % _leaseManagerHosts.Count].Suspects.Contains(host.Key))
                .Select(host => Task.Run(() =>
            {
                try
                {
                    host.Value.Decide(decideRequest);
                }
                catch (Grpc.Core.RpcException e)
                {
                    Console.WriteLine(e.Status);
                }
                //Console.WriteLine($"Successfuly sent decide request to ({host.Key})");
                return Task.CompletedTask;
            })))
            {
            }
        }

        public bool WaitForPaxos(SlotData slot)
        {
            Monitor.Enter(this);
            var success = true;
            Console.WriteLine("waiting for paxos");
            while (slot.IsPaxosRunning)
            {
                Monitor.Wait(this);

                //Console.WriteLine($"Curr.Slot ({_currentSlot}), Slot({slot.Slot}), Equals({(!slot.DecidedValues.Except(new List<Lease>()).Any() ? "true" : "false")})");
                // Slot ended without reaching consensus -> Do paxos again with another configuration
                if (_currentSlot > slot.Slot && !slot.DecidedValues.Except(new List<Lease>()).Any())
                {
                    Console.WriteLine(
                        $"Slot {slot.Slot} ended without consensus, starting a new paxos slot in slot {_currentSlot}.");
                    success = false;
                    break;
                }
            }
            Console.WriteLine("Paxos was sucessful!: + " + success);
            Monitor.Exit(this);
            return success;
        }

        public bool DoPaxosSlot()
        {
            ////Monitor.Enter(this);

            if (_bufferLeaseRequests.Count == 0)
            {
                Console.WriteLine("no lease requests to process");
                return true;
            }

            var slot = _slots[_currentSlot];

            // If paxos isn't running and a value hasn't been decided, start paxos
            if (!slot.IsPaxosRunning && slot.DecidedValues.SequenceEqual(new List<Lease>()))
            {
                //Console.WriteLine("Paxos starting");
                slot.IsPaxosRunning = true;
            }
            else if (!slot.IsPaxosRunning)
            {
                //Console.WriteLine("Paxos is not running and a value has been decided");
                return true;
            }

            // 1: who's the leader?
            var leader = int.MaxValue;
            for (int i = 0; i < _statePerSlot[_currentSlot - 1].Count; i++)
            {
                // If process is normal and not suspected by it's successor
                // A B C : B only becomes leader if C doesn't suspect it and all before are crashed
                if (_statePerSlot[_currentSlot - 1][i].Crashed == false &&
                    !_statePerSlot[_currentSlot - 1][i + 1].Suspects.Contains(_processBook[i]))
                {
                    leader = i;
                    break;
                }
            }

            if (leader == int.MaxValue)
            {
                //Console.WriteLine("No leader found"); // Should never happen
                Monitor.Exit(this);
                return false;
            }

            // 2: am I the Leader?
            if (_processId % _leaseManagerHosts.Count != leader)
            {
                //Console.WriteLine($"I'm not the leader, I'm process {_processId % _leaseManagerHosts.Count} and the leader is process {leader}");
                return WaitForPaxos(slot);
            }

            //Console.WriteLine($"Starting Paxos slot in slot {_currentSlot} for slot {_currentSlot}");

            Console.WriteLine($"Paxos leader is {leader} in slot {_currentSlot}");

            // Save processId for current paxos slot otherwise it might change in the middle of paxos if a new slot begins
            var leaderCurrentId = _processId;

            // Send prepare to all acceptors
            List<PromiseReply> promiseResponses = SendPrepareRequest(_currentSlot, leaderCurrentId);

            ////Monitor.Enter(this);
            // Stop being leader if there is a more recent one
            foreach (var response in promiseResponses)
            {
                if (response.ReadTimestamp > _processId)
                {
                    //Console.WriteLine($"I'm not the leader anymore, I'm process {_processId % _leaseManagerHosts.Count} and the leader is process {leader}");
                    return WaitForPaxos(slot);
                }
            }

            // Get values from promises
            var mostRecent = -1;
            var valueToPropose = new List<Lease>();
            foreach (var response in promiseResponses)
            {
                if (response.ReadTimestamp > mostRecent)
                {
                    mostRecent = response.ReadTimestamp;
                    valueToPropose = response.Leases.ToList();
                }
            }

            // If acceptors have no value, send own value
            if (!valueToPropose.Except(new List<Lease>()).Any())
            {
                int size = _bufferLeaseRequests.Count;
                for (int i = 0; i < size; i++)
                {
                    valueToPropose.Add(_bufferLeaseRequests[i].Lease);
                }
            }

            ////Monitor.Exit(this);
            // Send accept to all acceptors which will send decide to all learners
            SendAcceptRequest(_currentSlot, leaderCurrentId, valueToPropose);
            // Wait for learners to decide
            var retVal = WaitForPaxos(slot);
            return retVal;
        }

        public StatusUpdateResponse StatusUpdate()
        {
            Monitor.Enter(this);

            var slot = _currentSlot > 1 ? _slots[_currentSlot - 1] : _slots[_currentSlot];

            while (slot.IsPaxosRunning && slot.DecidedValues.SequenceEqual(new List<Lease>()))
            {
                Monitor.Wait(this);
            }

            Monitor.Exit(this);
            return new StatusUpdateResponse
            {
                Slot = slot.Slot,
                Leases = { slot.DecidedValues }
            };
        }

        public Empty LeaseRequest(LeaseRequest request)
        {
            Monitor.Enter(this);

            _bufferLeaseRequests.Add(request);

            Monitor.Exit(this);
            return new Empty();
        }
    }
}
