using Grpc.Core;
using TransactionManagerTransactionManagerProto;

namespace TKVTransactionManager.Services
{
    public class GossipService : Gossip.GossipBase
    {
        private readonly ServerService serverService;

        public GossipService(ServerService serverService)
        {
            this.serverService = serverService;
        }

        public override Task<PrepareResponse> Prepare(PrepareRequest request, ServerCallContext context)
        {
            return Task.FromResult(serverService.ReplyWithPrepare());
        }

        public override Task<CommitResponse> Commit(CommitRequest request, ServerCallContext context)
        {
            return Task.FromResult(serverService.CommitRequestReceived(request));
        }

        public override Task<UpdateResponse> Update(UpdateRequest request, ServerCallContext context)
        {
            return Task.FromResult(serverService.ReplyWithUpdate(request));
        }
        public override Task<SameSlotLeaseExecutionResponse> SameSlotLeaseExecution(SameSlotLeaseExecutionRequest request, ServerCallContext context)
        {
            return Task.FromResult(serverService.SameSlotLeaseExecution(request));
        }
    }
}