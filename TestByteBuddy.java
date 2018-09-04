package com.linkedin.beam;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.MethodVisitor;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;

import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.beam.sdk.util.common.ReflectHelpers.findClassLoader;

public class TestByteBuddy {

  public static class AddNum {
    private final int a;
    public AddNum(int a) {
      this.a = a;
    }

    public int add(int b, int c) {
      return a + b + c;
    }
  }

  public interface Invoker {
    int invokeAdd(int a, int b);
  }

  @Test
  public void testByteGen() {
    Class<? extends Invoker> invokerClass = genInvoker();
    try {
      AddNum add = new AddNum(1);
      Constructor<?> constructor = invokerClass.getConstructor(AddNum.class);
      Invoker invoker = (Invoker) constructor.newInstance(add);

      int r = invoker.invokeAdd(2, 3);
      System.out.println("invoked! result = " + r);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Class<? extends Invoker> genInvoker() {
    DynamicType.Builder<?> builder = new ByteBuddy()
        .with(new NamingStrategy.SuffixingRandom(Invoker.class.getSimpleName()))
        .subclass(Invoker.class)
        .defineField("a", new TypeDescription.ForLoadedType(AddNum.class), Visibility.PUBLIC)
        .defineConstructor(Visibility.PUBLIC)
        .withParameters(AddNum.class)
        .intercept(new InvokerConstructor())
        .defineMethod("invokeAdd", int.class, Visibility.PUBLIC)
        .withParameters(int.class, int.class)
        .intercept(new InvokerMethod());

    DynamicType.Unloaded<?> unloaded = builder.make();

    @SuppressWarnings("unchecked")
    Class<? extends Invoker> res =
        (Class<? extends Invoker>) unloaded
                .load(
                    findClassLoader(AddNum.class.getClassLoader()),
                    ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    return res;
  }

  private static final class InvokerConstructor implements Implementation {

    @Override
    public ByteCodeAppender appender(Target implementationTarget) {
      return new ByteCodeAppender() {
        @Override
        public Size apply(MethodVisitor methodVisitor,
                          Context implementationContext,
                          MethodDescription instrumentedMethod) {
          StackManipulation stackManipulation = new StackManipulation.Compound(
              MethodVariableAccess.REFERENCE.loadFrom(0),
              MethodInvocation.invoke(new TypeDescription.ForLoadedType(Object.class)
                .getDeclaredMethods().filter(isConstructor().and(takesArguments(0))).getOnly()),
              MethodVariableAccess.REFERENCE.loadFrom(0),
              MethodVariableAccess.REFERENCE.loadFrom(1),
              FieldAccess.forField(implementationTarget
                  .getInstrumentedType()
                  .getDeclaredFields()
                  .get(0)).write(),
              MethodReturn.VOID
          );

          StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
          return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
        }
      };
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }
  }

  private static final class InvokerMethod implements Implementation {

    @Override
    public ByteCodeAppender appender(Target implementationTarget) {
      return new ByteCodeAppender() {
        @Override
        public Size apply(MethodVisitor methodVisitor,
                          Context implementationContext,
                          MethodDescription instrumentedMethod) {
          try {
            MethodDescription targetMethod =
                new MethodDescription.ForLoadedMethod(
                    AddNum.class.getMethod("add", int.class, int.class));
            FieldDescription addField = implementationTarget
                .getInstrumentedType()
                .getDeclaredFields()
                .get(0);

            StackManipulation stackManipulation = new StackManipulation.Compound(
                MethodVariableAccess.REFERENCE.loadFrom(0),
                FieldAccess.forField(addField).read(),
                MethodVariableAccess.allArgumentsOf(targetMethod),
                MethodInvocation.invoke(targetMethod),
                Assigner.DEFAULT.assign(
                    targetMethod.getReturnType(), instrumentedMethod.getReturnType(), Assigner.Typing.STATIC),
                MethodReturn.of(instrumentedMethod.getReturnType())
            );

            StackManipulation.Size size = stackManipulation.apply(methodVisitor, implementationContext);
            return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }
  }
}
