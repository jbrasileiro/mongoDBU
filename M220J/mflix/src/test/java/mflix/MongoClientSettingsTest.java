package mflix;

import org.junit.Test;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.SslSettings;

public class MongoClientSettingsTest {

	@Test
	public void test() {
		MongoClientSettings settings = MongoClientSettings.builder()
			.applyConnectionString(new ConnectionString("mongodb+srv://mflixAppUser:mflixAppPwd@m220j-hejrs.mongodb.net/test?retryWrites=true&maxPoolSize=50&connectTimeoutMS=2000")).build();
		MongoClient mongoClient = MongoClients.create(settings);
		SslSettings sslSettings = settings.getSslSettings();
		ReadPreference readPreference = settings.getReadPreference();
		ReadConcern readConcern = settings.getReadConcern();
		WriteConcern writeConcern = settings.getWriteConcern();
		System.out.println(sslSettings.isEnabled());
		System.out.println(writeConcern.asDocument().toJson());
		System.out.println(readPreference.toString());
		System.out.println(writeConcern.asDocument().toJson());
		System.out.println(sslSettings.isInvalidHostNameAllowed());
	}
}
