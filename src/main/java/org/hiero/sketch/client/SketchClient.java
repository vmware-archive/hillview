package org.hiero.sketch.client;

import org.hiero.sketch.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Vector;
import java.util.stream.Collectors;

import akka.actor.*;
import akka.serialization.*;
import com.typesafe.config.*;
import scala.collection.generic.BitOperations;
import scala.collection.generic.ClassTagTraversableFactory;
import scala.collection.parallel.ParIterableLike;
import scala.reflect.ClassTag;

import static java.lang.System.err;

/**
 * Skeleton for the Hiero Client
 *
 */
public class SketchClient
{
/*
    public static void main( String[] args )
    {
        List<Integer> database = new ArrayList<Integer>();
        database.add(1);
        database.add(20);
        database.add(40);
        database.add(30);
        IPDS<List<Integer>> ipds = new LocalPDS<List<Integer>>(database);
        IMap<List<Integer>, List<Integer>> mapper = new CountMap("yolo");
        ipds.map(mapper);
        int result = ipds.sketch(new SumSketch());

        System.out.println( "Hello World! " + result );

        Config config = ConfigFactory.parseString("\n "+
                "akka { \n" +
                "    actor {\n" +
                "        serializers { \n" +
                "            java = \"akka.serialization.JavaSerializer\" \n" +
                "        }\n" +
                "        serialization-bindings {\n" +
                "            \"org.hiero.sketch*\" = java\n" +
                "        }\n" +
                "    }\n" +
                "} ");

        ActorSystem system = ActorSystem.create("example");

        // Get the Serialization Extension
        Serialization serialization = SerializationExtension.get(system);

        // Have something to serialize
        IMap<List<Integer>, List<Integer>> original = new CountMap("yolo");

        // Find the Serializer for it
        Serializer serializer = serialization.findSerializerFor(original);

        // Turn it into bytes
        byte[] bytes = serializer.toBinary(original);

        // Turn it back into an object,
        // the nulls are for the class manifest and for the classloader

        IMap<List<Integer>, List<Integer>> back = (CountMap) serializer.fromBinary(bytes);

        // Voilá!
        System.out.printf("received " + ((CountMap) back).s);


//        try {
//        try {
//            Class<?> c = Class.forName(CountMap.class.getName());
//            Object t = ((Class)((ParameterizedType) original.getClass().
//                    getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
//            System.out.println("aaa " + ((ParameterizedType)original.getClass()
//                    .getGenericSuperclass()).getActualTypeArguments());


//        } catch (InstantiationException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }

//            Method[] allMethods = c.getDeclaredMethods();
//            for (Method m : allMethods) {
//                String mname = m.getName();
//                if (!mname.startsWith("test")
//                        || (m.getGenericReturnType() != boolean.class)) {
//                    continue;
//                }
//                Type[] pType = m.getGenericParameterTypes();
//                if ((pType.length != 1)
//                        || Locale.class.isAssignableFrom(pType[0].getClass())) {
//                    continue;
//                }
//
//                System.out.format("invoking %s()%n", mname);
//                try {
//                    m.setAccessible(true);
//                    Object o = m.invoke(t, new Locale(args[1], args[2], args[3]));
//                    System.out.format("%s() returned %b%n", mname, (Boolean) o);
//
//                    // Handle any exceptions thrown by method to be invoked.
//                } catch (InvocationTargetException x) {
//                    Throwable cause = x.getCause();
//                    err.format("invocation of %s failed: %s%n",
//                            mname, cause.getMessage());
//                }
//            }

            // production code should handle these exceptions more gracefully
//        } catch (ClassNotFoundException x) {
//            x.printStackTrace();
//        } catch (InstantiationException x) {
//            x.printStackTrace();
//        } catch (IllegalAccessException x) {
//            x.printStackTrace();
//        }
    } */
}