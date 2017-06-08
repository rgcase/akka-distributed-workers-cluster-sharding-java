package worker;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Scheduler;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.UUID;

import scala.concurrent.duration.*;
import scala.concurrent.ExecutionContext;
import scala.concurrent.forkjoin.ThreadLocalRandom;
import worker.Master.Work;
import static worker.Frontend.NotOk;
import static worker.Frontend.Ok;

public class WorkProducer extends AbstractActor {

  private final ActorRef frontend;

  public WorkProducer(ActorRef frontend) {
    this.frontend = frontend;
  }

  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private Scheduler scheduler = getContext().system().scheduler();
  private ExecutionContext ec = getContext().system().dispatcher();
  private ThreadLocalRandom rnd = ThreadLocalRandom.current();
  private String nextWorkId() {
    return UUID.randomUUID().toString();
  }

  int n = 0;

  @Override
  public void preStart(){
    scheduler.scheduleOnce(Duration.create(5, "seconds"), getSelf(), Tick, ec, getSelf());
  }

  // override postRestart so we don't call preStart and schedule a new Tick
  @Override
  public void postRestart(Throwable reason) {
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().matchEquals(Tick, message -> {
      n += 1;
      log.info("Produced work: {}", n);
      Work work = new Work(nextWorkId(), n);
      frontend.tell(work, getSelf());
      getContext().become(waitAccepted(work), false);
    }).build();
  }

  private Receive waitAccepted(final Work work) {
    return receiveBuilder().match(Ok.class, message -> {
      getContext().unbecome();
      scheduler.scheduleOnce(Duration.create(rnd.nextInt(3, 10), "seconds"), getSelf(), Tick, ec, getSelf());
    }).match(NotOk.class, message -> {
      log.info("Work not accepted, retry after a while");
      scheduler.scheduleOnce(Duration.create(3, "seconds"), frontend, work, ec, getSelf());
    }).build();
  }

  private static final Object Tick = new Object() {
    @Override
    public String toString() {
      return "Tick";
    }
  };

}
