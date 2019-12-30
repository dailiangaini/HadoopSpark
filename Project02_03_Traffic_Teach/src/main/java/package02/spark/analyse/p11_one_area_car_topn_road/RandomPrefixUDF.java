package package02.spark.analyse.p11_one_area_car_topn_road;

import java.util.Random;

import org.apache.spark.sql.api.java.UDF2;

public class RandomPrefixUDF implements UDF2<String, Integer, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(String area_name_road_id, Integer ranNum) throws Exception {
		Random random = new Random();
		int prefix = random.nextInt(ranNum);
		return prefix+"_"+area_name_road_id;
	}

}
