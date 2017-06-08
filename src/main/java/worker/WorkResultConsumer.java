package worker;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import worker.Master.WorkResult;

public class WorkResultConsumer extends AbstractActor {

  private ActorRef mediator = DistributedPubSub.get(getContext().system()).mediator();
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  {
    mediator.tell(new DistributedPubSubMediator.Subscribe(Master.ResultsTopic, getSelf()), getSelf());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(DistributedPubSubMediator.SubscribeAck.class, message -> {}) // do nothing
            .match(WorkResult.class, workResult -> {
              log.info("Consumed result: {}", workResult.result);
            }).build();
  }
}
