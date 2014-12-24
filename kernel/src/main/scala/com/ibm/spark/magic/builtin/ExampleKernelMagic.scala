package com.ibm.spark.magic.builtin

import com.ibm.spark.magic.{KernelMagicTemplate, MagicOutput}

class ExampleKernelMagic extends KernelMagicTemplate {

  def execute(str: String, x: Int) : MagicOutput =
    MagicOutput("text/plain" -> s"string argument:${str}, int argument:${x}")

  def execute(str: String, x: Int, y: Int) : MagicOutput =
    MagicOutput("text/plain" -> s"args:${str}, ${x}, ${y}")

  def execute(x: Int) : MagicOutput =
    MagicOutput("text/plain" -> s"no string argument, int argument:${x}")

}
