package pagerankpig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRankLoad extends LoadFunc {
    private static final Log log = LogFactory.getLog(PageRankLoad.class);

    private final TupleFactory tupleFactory = TupleFactory.getInstance();
    private final BagFactory bagFactory = BagFactory.getInstance();

    @SuppressWarnings("rawtypes")
    private RecordReader reader;

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new XmlInputFormat();
    }

    @Override
    public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader recordReader, PigSplit pigSplit) throws IOException {
        this.reader = recordReader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            Tuple tuple = tupleFactory.newTuple(2);
            DataBag bag = bagFactory.newDefaultBag();

            if (!reader.nextKeyValue()) {
                return null;
            }

            Text page = (Text) reader.getCurrentValue();

            String pageTitle = new String();
            String pageContent = new String();

            int begin = page.find("<title>");
            int end = page.find("</title>", begin);

            pageTitle = Text.decode(page.getBytes(), begin + 7, end - (begin + 7));

            if (pageTitle.contains(":"))
                return tuple;

            begin = page.find(">", page.find("<text"));
            end = page.find("</title>", begin);

            if (begin + 1 == -1 || end == -1) {
                pageTitle = "";
                pageContent = "";
            }

            pageContent = Text.decode(page.getBytes(), begin + 1, end - (begin + 1));

            log.info("Page Title " + pageTitle);

            tuple.set(0, new DataByteArray(pageTitle.replace(' ', '_').toString()));

            Pattern findWikiLinks = Pattern.compile("\\[.+?\\]");

            Matcher matcher = findWikiLinks.matcher(pageContent);

            while (matcher.find()) {
                String linkPage = matcher.group();

                linkPage = getPage(linkPage);
                if (linkPage == null || linkPage.isEmpty())
                    continue;

                Tuple ltuple = tupleFactory.newTuple(1);
                ltuple.set(0, linkPage);
                bag.add(ltuple);
            }

            tuple.set(1, bag);
            return tuple;
        } catch (InterruptedException e) {
            throw new ExecException(e);
        }
    }

    private String getPage(String linkPage) {
        boolean isGoodLink = true;
        int srt = 1;
        if (linkPage.startsWith("[[")) {
            srt = 2;
        }

        int end = linkPage.indexOf("#") > 0 ? linkPage.indexOf('#') :
                    linkPage.indexOf('|') > 0 ? linkPage.indexOf('|') :
                        linkPage.indexOf("]") > 0 ? linkPage.indexOf("]") : linkPage.indexOf("|");

        if (linkPage.length() < srt + 2 || linkPage.length() > 100 || linkPage.contains(":") ||
                linkPage.contains(",") || linkPage.contains("&"))
            isGoodLink = false;

        char firstChar = linkPage.charAt(srt);

        if (firstChar == '#' || firstChar == ',' || firstChar == '.' || firstChar == '\'' ||
                firstChar == '-' || firstChar == '{')
            isGoodLink = false;

        if (!isGoodLink)
            return null;

        linkPage = linkPage.substring(srt, end);
        linkPage = linkPage.replaceAll("\\s", "_").replaceAll(",", "").replaceAll("&amp", "&");

        return linkPage;
    }
}
