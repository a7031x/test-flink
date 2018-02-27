import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.util.Random

case class StockPrice(symbol: String, price: Double)

class StockGenerator(symbol: String, sigma: Double) extends SourceFunction[StockPrice] {
  private var canceled = false

  override def cancel(): Unit = {
    canceled = true
  }

  override def run(sourceContext: SourceFunction.SourceContext[StockPrice]): Unit = {
    var price = 1000.0
    while (!canceled) {
      price = price + Random.nextGaussian * sigma
      sourceContext.collect(StockPrice(symbol, price))
      //Thread.sleep(1)
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
    //Thread.sleep(2000)
    (in.symbol, in.price)
  }
}

object MyApp extends App {
  def run(): Unit = {
    val cliParams: ParameterTool = ParameterTool.fromArgs(args)
    val productionMode = cliParams.getBoolean("productionMode", true)
    val checkpointPath = "file:///d:/temp/checkpoint-data"
    val env =
      if (productionMode) {
        StreamExecutionEnvironment.getExecutionEnvironment
      }
      else {
        val conf = new Configuration()
        conf.setString("state.checkpoints.dir", checkpointPath)
        conf.setInteger("state.checkpoints.num-retained", 2)
        StreamExecutionEnvironment.createLocalEnvironment(4, conf)
      }
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(Time.seconds(2).toMilliseconds)
    env.getCheckpointConfig.setCheckpointTimeout(Time.seconds(25).toMilliseconds)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setCheckpointInterval(Time.seconds(10).toMilliseconds)
    env.setStateBackend(new RocksDBStateBackend(checkpointPath, true))
    //env.setStateBackend(new FsStateBackend(checkpointPath, true))
    env.setParallelism(3)
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 6000))
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    createGraph(env)
    env.execute("test")
  }

  def createGraph(env: StreamExecutionEnvironment): Unit = {
    val SPX_Stream = env.addSource(new StockGenerator("SPX", 10))
    val FTSE_Stream = env.addSource(new StockGenerator("FTSE", 20))
    val DJI_Stream = env.addSource(new StockGenerator("DJI", 30))
    val BUX_Stream = env.addSource(new StockGenerator("BUX", 40))
    val MS_Stream = env.addSource(new StockGenerator("MS", 50))
    val DET_Stream = env.addSource(new StockGenerator("DET", 50))
    val source = SPX_Stream.union(
      FTSE_Stream, DJI_Stream, BUX_Stream, MS_Stream, DET_Stream)
    source
      .keyBy(_.symbol)
      .map(new TestMapFunction).name("map_count")
      .addSink(x => println(x._1, x._2)).name("sink")
  }

  run()
}
