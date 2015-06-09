import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class NLP implements SeepTask {

  private Schema schema = SchemaBuilder.getInstance()
      .newField(Type.STRING, "query").newField(Type.LONG, "ts")
      .newField(Type.LONG, "kb_time").build();

  private static StanfordCoreNLP pipeline;
  static Properties props = new Properties();
  static {
    props.setProperty("annotators",
        "tokenize, ssplit, pos, lemma, ner");
    pipeline = new StanfordCoreNLP(props);
  }

  static HashMap<String, String> pred_location = new HashMap<String, String>();
  static HashMap<String, String> pred_time = new HashMap<String, String>();
  static HashMap<String, String> pred_entity = new HashMap<String, String>();
  static HashMap<String, String> pred_other = new HashMap<String, String>();

  public static final String[] word_location = new String[] { "where" };
  public static final Set<String> q_location = new HashSet<String>(
      Arrays.asList(word_location));
  public static final String[] word_time = new String[] { "when" };
  public static final Set<String> q_time = new HashSet<String>(
      Arrays.asList(word_time));

  @Override
  public void setUp() {
    System.out.println("[NLP]");
    ExtractPredicates.getPredicates(pred_location, pred_time, pred_entity,
        pred_other);
  }

  @Override
  public void processData(ITuple data, API api) {
    long ts = System.currentTimeMillis();
    String text = data.getString("query");
    long kb_time = data.getLong("kb_time");
    String word = "";

    Annotation document = new Annotation(text);

    // run all Annotators on this text
    pipeline.annotate(document);

    // these are all the sentences in this document
    List<CoreMap> sentences = document.get(SentencesAnnotation.class);

    for (CoreMap sentence : sentences) {
      HashMap<String, String> predicates = new HashMap<String, String>();
      String subject = "";
      String match_pred = "";

      for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
        word = token.get(TextAnnotation.class);
        // this is the POS tag of the token
        String pos = token.get(PartOfSpeechAnnotation.class);
        String lemma = token.get(LemmaAnnotation.class);
        String ner = token.get(NamedEntityTagAnnotation.class);

        if (pos.equals("WRB")) {
          if (q_location.contains(lemma)) {
            predicates.putAll(pred_location);
          } else if (q_time.contains(lemma)) {
            predicates.putAll(pred_time);
          }
          continue;
        } else {
          predicates.putAll(pred_entity);
          predicates.putAll(pred_other);
        }
        if (!ner.equals("O")) {
          subject += lemma + "_";
        } else if (pos.equals(".") || pos.equals("WP")) {
          // ignore "What and full stops continue;
        } else {
          match_pred += lemma + " ";
        }
      }
      List<String> matched_pred = matchPred(match_pred, predicates);
      long te = System.currentTimeMillis();
      long nlp_time = te - ts;
      for (int i = 0; i < matched_pred.size(); ++i) {
        String sub_pred = subject.substring(0, subject.length() - 1)
            + "@" + matched_pred.get(i);
        //System.out.println("[sub_pred] " + sub_pred);
        byte[] processedData = OTuple.create(schema, new String[] {
            "query", "ts", "kb_time" }, new Object[] { sub_pred,
            nlp_time, kb_time });
        api.send(processedData);
      }
    }
  }

  /*
   * /** Calculates the similarity (a number within 0 and 1) between two
   * strings.
   */
  public static double similarity(String s1, String s2) {
    String longer = s1, shorter = s2;
    if (s1.length() < s2.length()) {
      longer = s2;
      shorter = s1;
    }
    int longerLength = longer.length();
    if (longerLength == 0) {
      return 1.0; /* both strings are zero length */
    }
    return (longerLength - editDistance(longer, shorter))
        / (double) longerLength;
  }

  public static int editDistance(String s1, String s2) {
    s1 = s1.toLowerCase();
    s2 = s2.toLowerCase();

    int[] costs = new int[s2.length() + 1];
    for (int i = 0; i <= s1.length(); i++) {
      int lastValue = i;
      for (int j = 0; j <= s2.length(); j++) {
        if (i == 0)
          costs[j] = j;
        else {
          if (j > 0) {
            int newValue = costs[j - 1];
            if (s1.charAt(i - 1) != s2.charAt(j - 1))
              newValue = Math.min(Math.min(newValue, lastValue),
                  costs[j]) + 1;
            costs[j - 1] = lastValue;
            lastValue = newValue;
          }
        }
      }
      if (i > 0)
        costs[s2.length()] = lastValue;
    }
    return costs[s2.length()];
  }

  private static List<String> matchPred(String word,
      HashMap<String, String> pred_set) {
    double max_score = 0.0;
    List<String> best_match = new ArrayList<String>();
    for (Object iter : pred_set.entrySet()) {
      HashMap.Entry pair = (HashMap.Entry) iter;
      String key = (String) pair.getKey();
      String value = (String) pair.getValue();
      double sim = similarity(word, value);
      if (sim > max_score) {
        max_score = sim;
        best_match.clear();
      }
      if (sim == max_score) {
        best_match.add(key);
      }
    }
    return best_match;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public void processDataGroup(ITuple arg0, API arg1) {
    // TODO Auto-generated method stub

  }

}

