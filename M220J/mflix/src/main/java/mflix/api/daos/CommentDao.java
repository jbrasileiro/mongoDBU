package mflix.api.daos;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import mflix.api.models.Comment;
import mflix.api.models.Critic;

@Component
public class CommentDao extends AbstractMFlixDao {

  public static String COMMENT_COLLECTION = "comments";

  private MongoCollection<Comment> commentCollection;

  private CodecRegistry pojoCodecRegistry;

  private final Logger log;

  @Autowired
  public CommentDao(
      MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
    super(mongoClient, databaseName);
    log = LoggerFactory.getLogger(this.getClass());
    this.db = this.mongoClient.getDatabase(MFLIX_DATABASE);
    this.pojoCodecRegistry =
        fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build()));
    this.commentCollection =
        db.getCollection(COMMENT_COLLECTION, Comment.class).withCodecRegistry(pojoCodecRegistry);
  }

  /**
   * Returns a Comment object that matches the provided id string.
   *
   * @param id - comment identifier
   * @return Comment object corresponding to the identifier value
   */
  public Comment getComment(String id) {
    return commentCollection.find(new Document("_id", new ObjectId(id))).first();
  }

  /**
   * Adds a new Comment to the collection. The equivalent instruction in the mongo shell would be:
   *
   * <p>db.comments.insertOne({comment})
   *
   * <p>
   *
   * @param comment - Comment object.
   * @throw IncorrectDaoOperation if the insert fails, otherwise
   * returns the resulting Comment object.
   */
  public Comment addComment(Comment comment) {
		if (comment.getId() == null || comment.getId().isEmpty()) {
			throw new IncorrectDaoOperation("Comment objects need to have an id field set.");
		}
	    try {
	        commentCollection.insertOne(comment);  
	        return comment;
	      } catch (MongoException e) {
	        log.error("An error ocurred while trying to insert a Comment.");
	        return null;
	      }
  }

  /**
   * Updates the comment text matching commentId and user email. This method would be equivalent to
   * running the following mongo shell command:
   *
   * <p>db.comments.update({_id: commentId}, {$set: { "text": text, date: ISODate() }})
   *
   * <p>
   *
   * @param commentId - comment id string value.
   * @param text - comment text to be updated.
   * @param email - user email.
   * @return true if successfully updates the comment text.
   */
  public boolean updateComment(String commentId, String text, String email) {
		log.error("Could not update comment `{}`. Make sure the comment is owned by `{}`",
			commentId, email);
	    try {
	    	Bson filter = Filters.and(Filters.eq("_id", new ObjectId(commentId)), Filters.eq("email", email));
	    	Bson updateObject = Updates.combine(Updates.set("text", text), Updates.set("date", new Date()));
	    	UpdateResult result = commentCollection.updateOne(
				filter,
				updateObject);
			if (result.getMatchedCount() > 0) {
				if (result.getModifiedCount() != 1) {
					log.warn("Comment `{}` text was not updated. Is it the same text?");
				}
				return true;
			}
			return false;
	      } catch (MongoException e) {
	        log.error("An error ocurred while trying to update a Comment.");
	        return false;
	      }
//		return result.getMatchedCount() > 0 && result.getModifiedCount() > 0;
  }

  /**
   * Deletes comment that matches user email and commentId.
   *
   * @param commentId - commentId string value.
   * @param email - user email value.
   * @return true if successful deletes the comment.
   */
  public boolean deleteComment(String commentId, String email) {
	    if(!Optional.ofNullable(commentId).isPresent()) {
	        throw new IllegalArgumentException("Commend id cannot be null");
	      }
		try {
			DeleteResult result = commentCollection
					.deleteOne(
						Filters.and( 
							Filters.eq("_id", new ObjectId(commentId)), 
							Filters.eq("email", email)));
			long count = result.getDeletedCount();
			if (count != 1) {
				log.error("Could not delete comment `{}` owned by `{}`", commentId, email);
				return false;
			} else {
				return true;
			}
		} catch (MongoException e) {
			log.error("An error ocurred while trying to delete a Comment.");
			return false;
		}
  }

  /**
   * Ticket: User Report - produce a list of users that comment the most in the website. Query the
   * `comments` collection and group the users by number of comments. The list is limited to up most
   * 20 commenter.
   *
   * @return List {@link Critic} objects.
   */
  public List<Critic> mostActiveCommenters() {
    List<Critic> mostActive = new ArrayList<>();
		Bson count = Aggregates.sortByCount("$email");
		Bson limit = Aggregates.limit(20);
		Bson sort = Aggregates.sort(Sorts.descending("count"));
		List<Bson> pipeline = Arrays.asList(count, limit, sort);
		commentCollection
			.withReadConcern(ReadConcern.MAJORITY)
			.aggregate(pipeline, Critic.class)
			.iterator()
			.forEachRemaining(mostActive::add);
    return mostActive;
  }
}
