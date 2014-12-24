package com.ibm.spark.magic

import com.ibm.spark.utils.DynamicReflectionSupport

import scala.language.dynamics

class MagicExecutor (val magicLoader: MagicLoader) extends Dynamic {
  val executeMethod = "execute"

  def applyDynamic(name: String)(args: Any*) : MagicOutput = {
    val magicClassName = magicLoader.magicClassName(name)
    magicLoader.hasMagic(magicClassName) match {
      case true  => executeMagic(magicClassName, args)
      case false => MagicOutput("text/plain" -> s"Magic ${magicClassName} not found.")
    }
  }

  private def executeMagic(magicClassName: String, args: Any*) : MagicOutput = {
    val inst = magicInstance(magicClassName)
    val dynamicSupport = new DynamicReflectionSupport(inst.getClass, inst)
    try {
      val dynamicResult = dynamicSupport.applyDynamic(executeMethod)(args)
      dynamicResult.asInstanceOf[MagicOutput]
    } catch {
      case t: Throwable => MagicOutput("text/plain" -> (s"Error: ${t.getMessage}"))
    }
  }

  private def magicInstance(name: String) : KernelMagicTemplate = {
    val magicClassName = magicLoader.magicClassName(name)
    magicLoader.createMagicInstance(magicClassName).asInstanceOf[KernelMagicTemplate]
  }

}
