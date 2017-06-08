package worker;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.cluster.sharding.ShardRegion;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.persistence.AbstractPersistentActor;

import java.io.Serializable;
import java.util.*;

import scala.collection.JavaConversions;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import static worker.MasterWorkerProtocol.*;

public class Master extends AbstractPersistentActor {

  public static String ResultsTopic = "results";

  public static Props props(FiniteDuration workTimeout) {
    return Props.create(Master.class, workTimeout);
  }

  private final FiniteDuration workTimeout;
  private final ActorRef mediator = DistributedPubSub.get(getContext().system()).mediator();
  private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private final Cancellable cleanupTask;

  private HashMap<String, WorkerState> workers = new HashMap<String, WorkerState>();
  private WorkState workState = new WorkState();

  public Master(FiniteDuration workTimeout) {
    this.workTimeout = workTimeout;
    ClusterClientReceptionist.get(getContext().system()).registerService(getSelf());
    this.cleanupTask = getContext().system().scheduler().schedule(workTimeout.div(2), workTimeout.div(2), getSelf(), CleanupTick, getContext().dispatcher(), getSelf());
  }

  @Override
  public void postStop() {
    cleanupTask.cancel();
  }

  private void notifyWorkers() {
    if (workState.hasWork()) {
      // could pick a few random instead of all
      for (WorkerState state : workers.values()) {
        if (state.status.isIdle())
          state.ref.tell(WorkIsReady.getInstance(), getSelf());
      }
    }
  }

  private static abstract class WorkerStatus {
    protected abstract boolean isIdle();

    private boolean isBusy() {
      return !isIdle();
    };

    protected abstract String getWorkId();

    protected abstract Deadline getDeadLine();
  }

  private static final class Idle extends WorkerStatus {
    private static final Idle instance = new Idle();

    public static Idle getInstance() {
      return instance;
    }

    @Override
    protected boolean isIdle() {
      return true;
    }

    @Override
    protected String getWorkId() {
      throw new IllegalAccessError();
    }

    @Override
    protected Deadline getDeadLine() {
      throw new IllegalAccessError();
    }

    @Override
    public String toString() {
      return "Idle";
    }
  }

  private static final class Busy extends WorkerStatus {
    private final String workId;
    private final Deadline deadline;

    private Busy(String workId, Deadline deadline) {
      this.workId = workId;
      this.deadline = deadline;
    }

    @Override
    protected boolean isIdle() {
      return false;
    }

    @Override
    protected String getWorkId() {
      return workId;
    }

    @Override
    protected Deadline getDeadLine() {
      return deadline;
    }

    @Override
    public String toString() {
      return "Busy{" + "work=" + workId + ", deadline=" + deadline + '}';
    }
  }

  private static final class WorkerState {
    public final ActorRef ref;
    public final WorkerStatus status;

    private WorkerState(ActorRef ref, WorkerStatus status) {
      this.ref = ref;
      this.status = status;
    }

    private WorkerState copyWithRef(ActorRef ref) {
      return new WorkerState(ref, this.status);
    }

    private WorkerState copyWithStatus(WorkerStatus status) {
      return new WorkerState(this.ref, status);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || !getClass().equals(o.getClass()))
        return false;

      WorkerState that = (WorkerState) o;

      if (!ref.equals(that.ref))
        return false;
      if (!status.equals(that.status))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = ref.hashCode();
      result = 31 * result + status.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "WorkerState{" + "ref=" + ref + ", status=" + status + '}';
    }
  }

  public static final Object CleanupTick = new Object() {
    @Override
    public String toString() {
      return "CleanupTick";
    }
  };

  public static final class Work implements Serializable {
    public final String workId;
    public final Object job;

    public Work(String workId, Object job) {
      this.workId = workId;
      this.job = job;
    }

    @Override
    public String toString() {
      return "Work{" + "workId='" + workId + '\'' + ", job=" + job + '}';
    }
  }

  public static final class WorkResult implements Serializable {
    public final String workId;
    public final Object result;

    public WorkResult(String workId, Object result) {
      this.workId = workId;
      this.result = result;
    }

    @Override
    public String toString() {
      return "WorkResult{" + "workId='" + workId + '\'' + ", result=" + result + '}';
    }
  }

  public static final class Ack implements Serializable {
    final String workId;

    public Ack(String workId) {
      this.workId = workId;
    }

    @Override
    public String toString() {
      return "Ack{" + "workId='" + workId + '\'' + '}';
    }
  }

  @Override
  public Receive createReceiveRecover() {
    return receiveBuilder()
            .match(WorkDomainEvent.class, arg0 -> {
              workState = workState.updated((WorkDomainEvent) arg0);
              log.info("Replayed {}", arg0.getClass().getSimpleName());
            }).build();
  }

  @Override
  public String persistenceId() {
    for (String role : JavaConversions.asJavaIterable((Cluster.get(getContext().system()).selfRoles()))) {
      if (role.startsWith("backend-")) {
        return role + "-master";
      }
    }
    return "master";

  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(RegisterWorker.class, cmd -> {
              String workerId = cmd.workerId;
              if (workers.containsKey(workerId)) {
                workers.put(workerId, workers.get(workerId).copyWithRef(getSender()));
              } else {
                log.info("Worker registered: {}", workerId);
                workers.put(workerId, new WorkerState(getSender(), Idle.instance));
                if (workState.hasWork()) {
                  getSender().tell(WorkIsReady.getInstance(), getSelf());
                }
              }
            }).match(WorkerRequestsWork.class, cmd -> {
              if (workState.hasWork()) {
                final String workerId = cmd.workerId;
                final WorkerState state = workers.get(workerId);
                if (state != null && state.status.isIdle()) {
                  final Work work = workState.nextWork();
                  persist(new WorkState.WorkStarted(work.workId), event -> {
                      workState = workState.updated(event);
                      log.info("Giving worker {} some work {}", workerId, event.workId);
                      workers.put(workerId, state.copyWithStatus(new Busy(event.workId, workTimeout.fromNow())));
                      getSender().tell(work, getSelf());
                  });
                }
              }
            }).match(WorkIsDone.class, cmd -> {
              final String workerId = cmd.workerId;
              final String workId = cmd.workId;
              if (workState.isDone(workId)) {
                getSender().tell(new Ack(workId), getSelf());
              } else if (!workState.isInProgress(workId)) {
                log.info("Work {} not in progress, reported as done by worker {}", workId, workerId);
              } else {
                log.info("Work {} is done by worker {}", workId, workerId);
                changeWorkerToIdle(workerId, workId);
                persist(new WorkState.WorkCompleted(workId,cmd.result), event -> {
                    workState = workState.updated(event);
                    mediator.tell(new DistributedPubSubMediator.Publish(ResultsTopic, new WorkResult(event.workId,event.result)), getSelf());
                    getSender().tell(new Ack(event.workId), getSelf());
                });
              }
            }).match(WorkFailed.class, cmd -> {
              final String workId = cmd.workId;
              final String workerId = cmd.workerId;
              if (workState.isInProgress(workId)) {
                log.info("Work {} failed by worker {}", workId, workerId);
                changeWorkerToIdle(workerId, workId);
                persist(new WorkState.WorkerFailed(workId), event -> {
                    workState = workState.updated(event);
                    notifyWorkers();
                });
              }
            }).match(Work.class, cmd -> {
              final String workId = cmd.workId;
              // idempotent
              if (workState.isAccepted(workId)) {
                getSender().tell(new Ack(workId), getSelf());
              } else {
                log.info("Accepted work: {}", workId);
                persist(new WorkState.WorkAccepted((Work) cmd), event -> {
                    // Ack back to original sender
                    getSender().tell(new Ack(event.work.workId), getSelf());
                    workState = workState.updated(event);
                    notifyWorkers();
                });
              }
            }).matchEquals(CleanupTick, cmd -> {
              Iterator<Map.Entry<String, WorkerState>> iterator = workers.entrySet().iterator();
              while (iterator.hasNext()) {
                Map.Entry<String, WorkerState> entry = iterator.next();
                String workerId = entry.getKey();
                WorkerState state = entry.getValue();
                if (state.status.isBusy()) {
                  if (state.status.getDeadLine().isOverdue()) {
                    log.info("Work timed out: {}", state.status.getWorkId());
                    workers.remove(workerId);
                    persist(new WorkState.WorkerTimedOut(state.status.getWorkId()), event -> {
                        workState = workState.updated(event);
                        notifyWorkers();
                    });
                  }
                }
              }
            }).build();
  }

  private void changeWorkerToIdle(String workerId, String workId) {
    if (workers.get(workerId).status.isBusy()) {
      workers.put(workerId, workers.get(workerId).copyWithStatus(new Idle()));
    }
  }

  public static ShardRegion.MessageExtractor messageExtractor(int numberOfMasters) {
    return new ShardRegion.MessageExtractor() {
      @Override
      public String entityId(Object message) { // Assumes workerIds and workIds are approximately uniformly distributed
        if (message instanceof RegisterWorker) {
          return String.valueOf(Math.abs(((RegisterWorker) message).workerId.hashCode()) % numberOfMasters);
        } else if (message instanceof WorkerRequestsWork) {
          return String.valueOf(Math.abs(((WorkerRequestsWork) message).workerId.hashCode()) % numberOfMasters);
        } else if (message instanceof WorkIsDone) {
          return String.valueOf(Math.abs(((WorkIsDone) message).workerId.hashCode()) % numberOfMasters);
        } else if (message instanceof WorkFailed) {
          return String.valueOf(Math.abs(((WorkFailed) message).workerId.hashCode()) % numberOfMasters);
        } else if (message instanceof Work) {
          return String.valueOf(Math.abs(((Work) message).workId.hashCode()) % numberOfMasters);
        } else return null;
      }

      @Override
      public String shardId(Object message) {
        return entityId(message); // Each shard will have a single entity
      }

      @Override
      public Object entityMessage(Object message) {
        return message;
      }
    };
  }
}
