package telran.aws.lambda;

import java.util.List;

import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

public class KinessisHandler implements RequestHandler<KinesisEvent, String> {

	private static final String TABLE_NAME = "sensor_data";

	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		var logger = context.getLogger();
		logger.log("handler started");
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient(); 
		DynamoDB dynamoDb = new DynamoDB(client);
		Table table = dynamoDb.getTable(TABLE_NAME);
		List<String> records = input.getRecords().stream()
				.map(r -> new String(r.getKinesis().getData().array())).toList();
		records.forEach(logger::log);
		return null;
	}

}
