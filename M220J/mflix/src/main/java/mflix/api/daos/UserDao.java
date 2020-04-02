package mflix.api.daos;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

import mflix.api.models.Session;
import mflix.api.models.User;

@Configuration
public class UserDao extends AbstractMFlixDao {

  private final MongoCollection<User> usersCollection;
  private final MongoCollection<Session> sessionsCollection;
  private final CodecRegistry codecRegistry;
  private final Logger log;

  @Autowired
  public UserDao(
      final MongoClient mongoClient, @Value("${spring.mongodb.database}") final String databaseName) {
    super(mongoClient, databaseName);
    log = LoggerFactory.getLogger(this.getClass());
    codecRegistry =
        fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build()));

    usersCollection = db.getCollection("users", User.class).withCodecRegistry(codecRegistry);
    sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(codecRegistry);
  }

  /**
   * Inserts the `user` object in the `users` collection.
   *
   * @param user - User object to be added
   * @return True if successful, throw IncorrectDaoOperation otherwise
   */
  public boolean addUser(final User user) {
		try {
			usersCollection
			.withWriteConcern(WriteConcern.MAJORITY)
			.insertOne(user);
			return true;
		} catch (MongoException e) {
			log.error("An error ocurred while trying to insert a User.");
			if (ErrorCategory.fromErrorCode(e.getCode()) == ErrorCategory.DUPLICATE_KEY) {
				throw new IncorrectDaoOperation("The User is already in the database.");
			}
			return false;
		}

  }

  /**
   * Creates session using userId and jwt token.
   *
   * @param userId - user string identifier
   * @param jwt - jwt string token
   * @return true if successful
   */
   	public boolean createUserSession(
   		final String userId,
   		final String jwt) {
   		try {
   			final Session session = new Session();
   			session.setUserId(userId);
   			session.setJwt(jwt);
   			if (Optional.ofNullable(sessionsCollection.find(Filters.eq("user_id", userId)).first())
   					.isPresent()) {
   				sessionsCollection.updateOne(Filters.eq("user_id", userId), Updates.set("jwt", jwt));
   			} else {
   				sessionsCollection.insertOne(session);
   			}
   			return true;
		} catch (MongoException e) {
			log.error("An error ocurred while trying to insert/update a Session.");
			return false;
		}
   	}

  /**
   * Returns the User object matching the an email string value.
   *
   * @param email - email string to be matched.
   * @return User object or null.
   */
  public User getUser(final String email) {
		User user = new User();
		user = usersCollection.find(Filters.eq("email", email)).first();
		return user;
  }

  /**
   * Given the userId, returns a Session object.
   *
   * @param userId - user string identifier.
   * @return Session object or null.
   */
	public Session getUserSession(
		final String userId) {
		return sessionsCollection.find(Filters.eq("user_id", userId)).first();
	}

	public boolean deleteUserSessions(
		final String userId) {
		sessionsCollection.deleteMany(Filters.eq("user_id", userId));
		return true;
	}

  /**
   * Removes the user document that match the provided email.
   *
   * @param email - of the user to be deleted.
   * @return true if user successfully removed
   */
  public boolean deleteUser(final String email) {
		try {
			sessionsCollection.deleteMany(Filters.eq("user_id", email));
			usersCollection.deleteMany(Filters.eq("email", email));
			return true;
		} catch (MongoException e) {
			log.error("An error ocurred while trying to delete a User.");
			return false;
		}
  }

  /**
   * Updates the preferences of an user identified by `email` parameter.
   *
   * @param email - user to be updated email
   * @param userPreferences - set of preferences that should be stored and replace the existing
   *     ones. Cannot be set to null value
   * @return User object that just been updated.
   */
  public boolean updateUserPreferences(final String email, final Map<String, ?> userPreferences) {
		if (Objects.isNull(userPreferences)) {
			throw new IncorrectDaoOperation("userPreferences cannot be set to null");
		}
//		boolean result = usersCollection
//			.updateOne(Filters.eq("email", email),
//				Updates.set("preferences",
//					Optional.ofNullable(userPreferences).orElseThrow(
//						() -> new IncorrectDaoOperation("user preferences cannot be null"))))
//			.wasAcknowledged();
	    try {
	    	Bson filter = new Document("email", email);
	    	Bson updateObject = Updates.set("preferences", userPreferences);
	    	UpdateResult result = usersCollection.updateOne(filter, updateObject);
	    	if (result.getModifiedCount() < 1) {
	    		log.warn(
	    			"User `{}` was not updated. Trying to re-write the same `preferences` field: `{}`",
	    			email, userPreferences);
	    	}
			return true;
	    } catch (MongoException e) {
	        log.error("An error ocurred while trying to update User preferences.");
	        return false;
	      }
  }
}
