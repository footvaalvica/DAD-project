using System.Runtime.CompilerServices;
using Grpc.Core;
using LeaseManagerLeaseManagerServiceProto;

namespace TKVLeaseManager.Services
{
    public class PaxosService : Paxos.PaxosBase
    {
        private readonly LeaseManagerService _leaseManagerService;

        public PaxosService(LeaseManagerService leaseManagerService)
        {
            _leaseManagerService = leaseManagerService;
        }

        public override Task<PromiseReply> Prepare(PrepareRequest request, ServerCallContext context)
        {
            return Task.FromResult(_leaseManagerService.PreparePaxos(request));
        }

        public override Task<AcceptedReply> Accept(AcceptRequest request, ServerCallContext context)
        {
            return Task.FromResult(_leaseManagerService.AcceptPaxos(request));
        }

        public override Task<DecideReply> Decide(DecideRequest request, ServerCallContext context)
        {
            return Task.FromResult(_leaseManagerService.DecidePaxos(request));
        }

        public override Task<LeaderElectionReply> LeaderElection(LeaderElectionRequest request,
            ServerCallContext context)
        {
            return Task.FromResult(_leaseManagerService.LeaderElection(request));
        }
    }
}