import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

import scala.util.Random

case class StockPrice(symbol: String, price: Double, timestamp: Long)

class StockGenerator(symbol: String, sigma: Double) extends ParallelSourceFunction[StockPrice] {
  private var canceled = false
  private var size = 0
  override def cancel(): Unit = {
    canceled = true
  }

  override def run(sourceContext: SourceFunction.SourceContext[StockPrice]): Unit = {
    var price = 1000.0
    while (!canceled) {
      price = price + Random.nextGaussian * sigma
      val now = System.currentTimeMillis
      val rand = new Random().nextInt() % 5000
      size += 1
      println("produce", size, (now - rand) % 100000)
      sourceContext.collectWithTimestamp(StockPrice(symbol, price, now - rand), now - rand)
      Thread.sleep(1000)
    }
  }
}

class TestMapFunction extends RichMapFunction[StockPrice, (String, Double)] {
  var state: ListState[Double] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val descriptor = new ListStateDescriptor[Double](
      "sum",
      TypeInformation.of(new TypeHint[Double]{}))
    state = getRuntimeContext.getListState(descriptor)
  }

  override def map(in: StockPrice): (String, Double) = {
    state.add(in.price)
    (in.symbol, in.price)
  }
}

class TestProcessFunction extends ProcessFunction[StockPrice, (String, Long)] {

  var state: ValueState[String] = _
  var timer: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[String](
      "sum",
      TypeInformation.of(new TypeHint[String]{}))
    state = getRuntimeContext.getState(descriptor)
    val timerDesc = new ValueStateDescriptor[Long]("timer", TypeInformation.of(new TypeHint[Long]{}))
    timer = getRuntimeContext.getState(timerDesc)
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[StockPrice, (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {

    out.collect((state.value(), timestamp))
    println("----process event", state.value(), timestamp % 100000)
  }

  override def processElement(i: StockPrice, context: ProcessFunction[StockPrice, (String, Long)]#Context, collector: Collector[(String, Long)]): Unit = {
    val now = System.currentTimeMillis
    context.timerService().registerEventTimeTimer(now)
    timer.update(now)
    state.update(i.symbol)
 //   println("receive event", i.symbol, now % 100000)
  }
}

object MyApp extends App {
  def run(): Unit = {
    val cliParams: ParameterTool = ParameterTool.fromArgs(args)
    val productionMode = cliParams.getBoolean("productionMode", true)
    val checkpointPath = "file:///" + System.getProperty("java.io.tmpdir") + "checkpoint"
    print("checkpoint folder", checkpointPath)
    val env =
      if (productionMode) {
        StreamExecutionEnvironment.getExecutionEnvironment
      }
      else {
        val conf = new Configuration()
        conf.setString("state.checkpoints.dir", checkpointPath)
        conf.setInteger("state.checkpoints.num-retained", 4)
        StreamExecutionEnvironment.createLocalEnvironment(4, conf)
      }
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(Time.seconds(2).toMilliseconds)
    env.getCheckpointConfig.setCheckpointTimeout(Time.seconds(25).toMilliseconds)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setCheckpointInterval(Time.seconds(10).toMilliseconds)
    env.setStateBackend(new FsStateBackend(checkpointPath, false))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.setStateBackend(new FsStateBackend(checkpointPath, true))
    env.setParallelism(1)
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(0, 60000))
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    createGraph(env)
    env.execute("test")
  }

  def createGraph(env: StreamExecutionEnvironment): Unit = {
    val SPX_Stream = env.addSource(new StockGenerator("SPX", 10))
    val FTSE_Stream = env.addSource(new StockGenerator("FTSE", 20))
 //   val DJI_Stream = env.addSource(new StockGenerator("DJI", 30)).setParallelism(3)
 //   val BUX_Stream = env.addSource(new StockGenerator("BUX", 40)).setParallelism(1).name("BUX")
 //   val MS_Stream = env.addSource(new StockGenerator("MS", 50)).setParallelism(2)
 //   val DET_Stream = env.addSource(new StockGenerator("DET", 50))
 //   val source = SPX_Stream.union(
  //    FTSE_Stream, DJI_Stream, BUX_Stream, MS_Stream, DET_Stream)
    val source = SPX_Stream
    source
        .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[StockPrice] {
          override def checkAndGetNextWatermark(t: StockPrice, l: Long): Watermark = {
            if(l - System.currentTimeMillis() >= -1000) {
              println("emit watermark", l % 100000)
              new Watermark(l)
            }
            else
              new Watermark(0)
          }

          override def extractTimestamp(t: StockPrice, l: Long): Long = {
            t.timestamp
          }
        })
      .keyBy(_.symbol)
      .process(new TestProcessFunction)
      .addSink(new SinkFunction[(String, Long)] {
        private var count = 0
        override def invoke(value: (String, Long), context: SinkFunction.Context[_]): Unit = {
          count += 1
          println("consume", count)
        }
      }).name("sink")
  }

  run()
}
