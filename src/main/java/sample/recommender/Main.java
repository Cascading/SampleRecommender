/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package sample.recommender;

import java.util.Locale;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.aggregator.Count;
import cascading.operation.assertion.AssertMatches;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;


public class Main
  {
  public static int MIN_COMMON_TOKENS = 4;
  public static double MAX_SIMILARITY = 0.990;
  public static double MIN_SIMILARITY = 0.010;

  public static void main( String[] args )
    {
    String stopWords = args[ 0 ];
    String tweetPath = args[ 1 ];
    String tokenPath = args[ 2 ];
    String similarityPath = args[ 3 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    FlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create SOURCE taps, and read from local file system if inputs are not URLs
    Tap tweetTap = makeTap( tweetPath, new TextDelimited( true, "\t" ) );

    Tap stopTap = makeTap( stopWords, new TextDelimited( new Fields( "stop" ), true, "\t" ) );

    // create SINK taps, replacing previous output if needed
    Tap tokenTap = new Hfs( new TextDelimited( true, "\t" ), tokenPath, SinkMode.REPLACE );
    Tap similarityTap = new Hfs( new TextDelimited( true, "\t" ), similarityPath, SinkMode.REPLACE );

    /*
    flow part #1
    generate a bipartite map of (uid, token), while filtering out stop-words
    */

    // create a STREAM ASSERTION to validate the input data
    Pipe tweetPipe = new Pipe( "tweet" ); // name branch
    AssertMatches assertMatches = new AssertMatches( ".{6,150}" );
    tweetPipe = new Each( tweetPipe, AssertionLevel.STRICT, assertMatches );

    // create an OPERATION split the text into a token stream
    RegexSplitGenerator splitter = new RegexSplitGenerator( new Fields( "token" ), " " );
    Fields outputSelector = new Fields( "uid", "token" );
    tweetPipe = new Each( tweetPipe, new Fields( "text" ), splitter, outputSelector );

    tweetPipe = new Unique( tweetPipe, Fields.ALL );

    RegexFilter filter = new RegexFilter( "^\\S\\S+$" );
    tweetPipe = new Each( tweetPipe, new Fields( "token" ), filter );

    // create PIPEs for left join on the stop words
    Pipe stopPipe = new Pipe( "stop" ); // name branch
    Pipe joinPipe = new HashJoin( tweetPipe, new Fields( "token" ), stopPipe, new Fields( "stop" ), new LeftJoin() );
    joinPipe = new Each( joinPipe, new Fields( "stop" ), new RegexFilter( "^$" ) );

    joinPipe = new Retain( joinPipe, new Fields( "uid", "token" ) );

    /*
    flow part #2
    create SINK tap to measure token frequency, which will need to be used to adjust
    stop words -- based on an R script
    */

    Pipe tokenPipe = new Pipe( "token", joinPipe ); // name branch
    tokenPipe = new GroupBy( tokenPipe, new Fields( "token" ) );
    tokenPipe = new Every( tokenPipe, Fields.ALL, new Count(), Fields.ALL );

    /*
    flow part #3
    generate an inverted index for ((uid1,uid2), token) to avoid having to perform
    a cross-product, which would impose a bottleneck in the parallelism
    */

    Pipe invertPipe = new Pipe( "inverted index", joinPipe );
    invertPipe = new CoGroup( invertPipe, new Fields( "token" ), 1, new Fields( "uid1", "ignore", "uid2", "token" ) );

    Fields filterArguments = new Fields( "uid1", "uid2" );
    String uidFilter = "uid1.compareToIgnoreCase( uid2 ) >= 0";
    invertPipe = new Each( invertPipe, filterArguments, new ExpressionFilter( uidFilter, String.class ) );
    Fields ignore = new Fields( "ignore" );
    invertPipe = new Discard( invertPipe, ignore );

    /*
    flow part #4
    count the number of tokens in common for each uid pair and apply a threshold
    */

    Pipe commonPipe = new GroupBy( new Pipe( "uid common", invertPipe ), new Fields( "uid1", "uid2" ) );
    commonPipe = new Every( commonPipe, Fields.ALL, new Count( new Fields( "common" ) ), Fields.ALL );

    String commonFilter = String.format( "common < %d", MIN_COMMON_TOKENS );
    commonPipe = new Each( commonPipe, new Fields( "common" ), new ExpressionFilter( commonFilter, Integer.TYPE ) );

    /*
    flow part #5
    count the number of tokens overall for each uid, then join to calculate
    the vector length for uid1
    */

    Fields tokenCount = new Fields( "token_count" );
    Pipe countPipe = new GroupBy( "count", joinPipe, new Fields( "uid" ) );
    countPipe = new Every( countPipe, Fields.ALL, new Count( tokenCount ), Fields.ALL );

    joinPipe = new CoGroup( countPipe, new Fields( "uid" ), commonPipe, new Fields( "uid1" ) );
    joinPipe = new Pipe( "common", joinPipe );
    joinPipe = new Discard( joinPipe, new Fields( "uid" ) );

    joinPipe = new Rename( joinPipe, tokenCount, new Fields( "token_count1" ) );

    /*
    flow part #6 join to be able to calculate the vector length for
    uid2, remove instances where one uid merely retweets another,
    then calculate an Ochiai similarity metric to find the nearest
    "neighbors" for each uid -- as recommended users to "follow"
    */

    joinPipe = new CoGroup( "similarity", countPipe, new Fields( "uid" ), joinPipe, new Fields( "uid2" ) );

    joinPipe = new Rename( joinPipe, tokenCount, new Fields( "token_count2" ) );

    // use a DEBUG to check the values in the tuple stream; turn off in the FLOWDEF below
    joinPipe = new Each( joinPipe, DebugLevel.VERBOSE, new Debug( true ) );

    Fields expressionArguments = new Fields( "token_count1", "token_count2", "common" );
    commonFilter = "( token_count1 == common ) || ( token_count2 == common )";
    joinPipe = new Each( joinPipe, expressionArguments, new ExpressionFilter( commonFilter, Integer.TYPE ) );

    Fields ochiaiArguments = new Fields( "uid1", "token_count1", "uid2", "token_count2", "common" );
    Fields resultFields = new Fields( "uid", "recommend_uid", "similarity" );
    joinPipe = new Each( joinPipe, ochiaiArguments, new OchiaiFunction( resultFields ), Fields.RESULTS );

    /*
    flow part #7
    apply thresholds to filter out poor recommendations
    */

    Fields similarityArguments = new Fields( "similarity" );
    commonFilter = String.format(Locale.US, "similarity < %f || similarity > %f", MIN_SIMILARITY, MAX_SIMILARITY );
    joinPipe = new Each( joinPipe, similarityArguments, new ExpressionFilter( commonFilter, Double.TYPE ) );

    /*
    connect up all the flow, generate a flow diagram, then run the flow.
    results for recommended users get stored in the "similarityPath" sink tap.
    */

    FlowDef flowDef = FlowDef.flowDef().setName( "similarity" );
    flowDef.addSource( tweetPipe, tweetTap );
    flowDef.addSource( stopPipe, stopTap );
    flowDef.addTailSink( tokenPipe, tokenTap );
    flowDef.addTailSink( joinPipe, similarityTap );

    // set to DebugLevel.VERBOSE for trace, or DebugLevel.NONE in production
    flowDef.setDebugLevel( DebugLevel.VERBOSE );

    // set to AssertionLevel.STRICT for all assertions, or AssertionLevel.NONE in production
    flowDef.setAssertionLevel( AssertionLevel.STRICT );

    Flow similarityFlow = flowConnector.connect( flowDef );
    similarityFlow.writeDOT( "dot/similarity.dot" );
    similarityFlow.complete();
    }

  public static Tap makeTap( String path, Scheme scheme )
    {
    return path.matches( "^[^:]+://.*" ) ? new Hfs( scheme, path ) : new Lfs( scheme, path );
    }
  }
