/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package sample.recommender;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/**
 * see also:
 * http://www.mothur.org/wiki/Ochiai
 * http://en.wikipedia.org/wiki/Cosine_similarity#Ochiai_coefficient
 */

public class OchiaiFunction extends BaseOperation implements Function
  {
  public OchiaiFunction( Fields fieldDeclaration )
    {
    super( 5, fieldDeclaration );
    }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry argument = functionCall.getArguments();

    String uid1 = argument.getString( 0 );
    int token_count1 = argument.getInteger( 1 );
    String uid2 = argument.getString( 2 );
    int token_count2 = argument.getInteger( 3 );
    int common = argument.getInteger( 4 );

    double similarity = calcSimilarity( token_count1, token_count2, common );

    Tuple result = new Tuple();
    result.add( uid1 );
    result.add( uid2 );
    result.add( similarity );
    functionCall.getOutputCollector().add( result );

    result = new Tuple();
    result.add( uid2 );
    result.add( uid1 );
    result.add( similarity );
    functionCall.getOutputCollector().add( result );
    }

  public double calcSimilarity( int token_count1, int token_count2, int common )
    {
    return (double) common / Math.sqrt( (double) token_count1 * token_count2 );
    }
  }
