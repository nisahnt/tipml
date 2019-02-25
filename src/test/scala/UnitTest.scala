
import com.ml.gyc.{TaxiTipPredict, TaxiTipTrainer}
import org.junit.{Test, Ignore}


class UnitTest {

 // System.setProperty("hadoop.home.dir", "C:\\hadoop_home\\")
  //System.setProperty("java.library.path", "C:\\hadoop_home\\bin")
  @Ignore
  @Test
  def testTaxiTipTrainer {
    TaxiTipTrainer.main(Array("local", "src/test/resources/sourceWarehouseDir/2017_Green_Taxi_Trip_Data-part1.csv", "src/test/resources/destinationWarehouseDir/model"))
  }

  @Ignore
  @Test
  def testTaxiTipPredictor {
    TaxiTipPredict.main(Array("local", "src/test/resources/sourceWarehouseDir/2017_Green_Taxi_Trip_Data-part2.csv", "src/test/resources/destinationWarehouseDir/model", "src/test/resources/destinationWarehouseDir/out"))
  }
}
