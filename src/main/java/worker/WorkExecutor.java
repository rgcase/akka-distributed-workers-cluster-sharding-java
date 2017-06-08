package worker;

import akka.actor.UntypedAbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class WorkExecutor extends UntypedAbstractActor {

  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  @Override
  public void onReceive(Object message) {
    if (message instanceof Integer) {
      Integer n = (Integer) message;
      int n2 = n.intValue() * n.intValue();
      String result = n + " * " + n + " = " + n2;
      log.info("Produced result {}", result);
      getSender().tell(new Worker.WorkComplete(result), getSelf());
    }
  }
}
