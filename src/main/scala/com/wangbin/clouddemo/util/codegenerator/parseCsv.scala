package com.wangbin.clouddemo.util.codegenerator

import java.io.{BufferedReader, File, FileWriter, InputStreamReader}

import scala.io.Source

object parseCsv {
	//全局控制变量
	val sNameColumnNum = 1 //源表字段名所在的列数(第几列)
	val sTypeColumnNum = 2 //字段类型所在的列数(第几列)
	val sCommentColumnNum = 3 //字段类型所在的列数(第几列)
	val dNameColumnNum = 4 //目标字段名所在的列数(第几列)
	val objColumnNum = 5 //生成obj配置所在CSV文件的列数(第几列)
	val sep = System.getProperty("line.separator") //获取操作系统的换行符
	val tab = "\t" //制表符

	//	//时间类型
	//	var shasTimestamp = true
	//	var ohasTimestamp = true

	/**
		* 代码生成入口,校验参数合法性
		* 判断是否存在解析路径和输出文件路径
		*
		* @param args 获取传递的参数信息[解析文件路径,输出文件路径]
		*/
	def initParse(args: Array[_]): Unit = {
		if (args.toList.length == 0) {
			println("参数为空---->>请配置解析路径和输出文件路径")
			System.exit(0)
		} else if (args.toList.length == 1) {
			println("解析路径为: " + args.toList(0))
			println("未配置输出文件路径,是否配置为解析路径(y/n)?")
			val br = new BufferedReader(new InputStreamReader(System.in));
			val line = br.readLine()
			if (line.toUpperCase.equals("Y")) {
				println("+++++++++++++++++ code generation start +++++++++++++++++")
				val inPath = args(0).toString
				val outPath = args(0).toString
				println("inPath:\t" + inPath + sep + "outPath:\t" + outPath)
				val files = subDir(new File(inPath), "csv").toList
				parseFiles(files, outPath)
				println("+++++++++++++++++ code generation end +++++++++++++++++")
			} else {
				println("请手动配置输出文件路径!")
				System.exit(0)
			}
		} else {
			println("+++++++++++++++++ code generation start +++++++++++++++++")
			val inPath = args.toList(0).toString
			val outPath = args.toList(1).toString
			println("\tinPath:\t" + inPath + sep + "\toutPath:\t" + outPath)
			val files = parseCsv.subDir(new File(inPath), "csv").toList
			parseFiles(files, outPath)
			println("+++++++++++++++++ code generation end    +++++++++++++++++")
		}
	}


	/**
		* 对指定的配置文件进行解析,解析完将结果输出到指定的目录
		*
		* @param files      需要解析的文件路径
		* @param outputPath 输出目录
		*/
	def parseFiles(files: List[File], outputPath: String): Unit = {
		if (files.isEmpty) {
			println("未匹配到文件,请查看解析路径下是否存在相关配置文件")
			System.exit(0)
		}

		println(s"\t需要解析的配置文件: $sep\t" + files.mkString(s"$sep\t"))
		for (inputPath <- files) {
			codeGenerator(inputPath.getPath, outputPath)
		}
	}


	/**
		* 通过解析inputPath文件,生成相关case class,输出到给定outputPath路径下
		*
		* @param inputPath  配置文件路径
		* @param outputPath 输出路径
		*/
	def codeGenerator(inputPath: String, outputPath: String): Unit = {
		//获取需要解析的路径
		println(s"*=======current path : $inputPath")
		val srcFile = Source.fromFile(inputPath)
		val srcArr = srcFile.getLines().toList
		srcFile.close()
		//创建输出文件信息
		val caseBuilder: StringBuilder = new StringBuilder
		val objBuilder: StringBuilder = new StringBuilder
		val selectBuilder: StringBuilder = new StringBuilder
		//获取输出文件名
		val caseName = new File(inputPath).getName.replace(".csv", "")
		println("\tcaseName-->" + caseName)
		val objName = srcArr(0).split(",")(objColumnNum - 1)
		println("\tobjName-->" + objName)
		//获取输出文件包名
		val packageName = outputPath.split("scala")(1).tail.replace(File.separator, ".")
		println("\tpackageName:" + packageName)
		val fieldsNum = srcArr.length
		caseBuilder.append(s"case class $caseName ( $sep")
		objBuilder.append(s"case class $objName ( $sep")
		selectBuilder.append("\t/*" + sep)

		for (index <- 1 until fieldsNum) {
			val line2arr: Array[String] = srcArr(index).split(",")
			line2arr(1).toUpperCase match {
				case "STRING" => appendCode(line2arr, index, fieldsNum, caseBuilder, objBuilder, selectBuilder, "String")
				case "INT" => appendCode(line2arr, index, fieldsNum, caseBuilder, objBuilder, selectBuilder, "Int")
				case "DOUBLE" => appendCode(line2arr, index, fieldsNum, caseBuilder, objBuilder, selectBuilder, "Double")
				case "TIMESTAMP" => appendCode(line2arr, index, fieldsNum, caseBuilder, objBuilder, selectBuilder, "Timestamp")

			}
		}

		//		var arrSelectBuilder = selectBuilder.toString().split(s"$sep")
		//		for (index <- 0 until arrSelectBuilder.length) {
		//			if (index == arrSelectBuilder.length - 1) {
		//				objBuilder.append(arrSelectBuilder(index).replace(",", " ") + sep)
		//			} else {
		//				objBuilder.append(arrSelectBuilder(index) + sep)
		//			}
		//		}
		deleteLastComma(selectBuilder)
		deleteLastComma(objBuilder)
		caseBuilder.append(s")$sep") insert(0, "import java.sql.Timestamp" + sep)
		caseBuilder.insert(0, s"package $packageName $sep")
		objBuilder.append(s")$sep").append(selectBuilder).append(s"\t*/$sep").insert(0, s"import java.sql.Timestamp $sep")
		objBuilder.insert(0, s"package $packageName $sep")
		//写入文件前查看该文件是否存在
		val casePath = new File(outputPath + File.separator + caseName + ".scala").getAbsolutePath
		val objPath = new File(outputPath + File.separator + objName + ".scala").getAbsolutePath
		deleteFile(casePath)
		deleteFile(objPath)
		writeBuilder(caseBuilder, casePath)
		writeBuilder(objBuilder, objPath)
		println(s"\tCode generation completed :)")
		//		println(caseBuilder)
		//		println("=======================================")
		//		println(objBuilder)
	}


	/**
		* 根据配置文件中的字段信息添加到case class中
		*
		* @param arr       配置文件中的一条配置信息集合
		* @param index     当前处理的配置文件行号
		* @param case_type 配置文件中的字段类型
		*/
	def appendCode(arr: Array[String], index: Int, fieldsNum: Int, caseBuilder: StringBuilder, objBuilder: StringBuilder, selectBuilder: StringBuilder, case_type: String): Unit = {
		/*有逗号追加*/
		def hasCommaAppend(builder: StringBuilder, columnNum: Int): Unit = {
			builder.append(tab + arr(columnNum - 1) + s":$case_type, //" + arr(sCommentColumnNum - 1) + sep)
		}

		/*无逗号追加*/
		def noCommaAppend(builder: StringBuilder, columnNum: Int): Unit = {
			builder.append(tab + arr(columnNum - 1) + s":$case_type //" + arr(sCommentColumnNum - 1) + sep)
		}

		/*select内容追加*/
		def selectAppend(builder: StringBuilder): Unit = {
			builder.append(tab + "\"" + arr(sNameColumnNum - 1) + "\"" + " as " + "\"" + arr(dNameColumnNum - 1) + "\"" + ", //" + arr(sCommentColumnNum - 1) + sep)
		}

		/*import class*/
		//		def importType(arr: Array[String], builder: StringBuilder, inType: String, hasType: Boolean, comment: String):Unit = {
		//			if (arr(1).toUpperCase.equals(inType) && hasType) {
		//				builder.insert(0, sep)
		//				builder.insert(1, comment + sep)
		//				hasType = false
		//			}
		//		}
		//
		//		importType(arr, caseBuilder, "TIMESTAMP", shasTimestamp, "import java.sql.Timestamp")
		//		importType(arr, objBuilder, "TIMESTAMP", ohasTimestamp, "import java.sql.Timestamp")

		//		if (arr(sTypeColumnNum - 1).toUpperCase.equals("TIMESTAMP") && shasTimestamp) {
		//			caseBuilder.insert(0, sep)
		//			caseBuilder.insert(1, "import java.sql.Timestamp" + sep)
		//			shasTimestamp = false
		//		}
		//
		//		if (arr(sTypeColumnNum).toUpperCase.equals("TIMESTAMP") && Array("TRUE", "T").contains(arr(objColumnNum - 1).toUpperCase) && ohasTimestamp) {
		//			objBuilder.insert(0, sep)
		//			objBuilder.insert(1, "import java.sql.Timestamp" + sep)
		//			ohasTimestamp = false
		//		}

		if ((arr(sTypeColumnNum - 1).charAt(0).toUpper + arr(1).substring(1).toLowerCase).equals(case_type)) {
			if (index < fieldsNum - 1) {
				hasCommaAppend(caseBuilder, sNameColumnNum)
				if (Array("TRUE", "T").contains(arr(objColumnNum - 1).toUpperCase)) {
					hasCommaAppend(objBuilder, dNameColumnNum)
					selectAppend(selectBuilder)
				}
			} else {
				noCommaAppend(caseBuilder, sNameColumnNum)
				if (Array("TRUE", "T").contains(arr(4).toUpperCase)) {
					hasCommaAppend(objBuilder, dNameColumnNum)
					selectAppend(selectBuilder)
				}
			}
		}
	}


	/**
		* 过滤给定路径下的特定文件类型集合
		*
		* @param dir       需要解析的路径
		* @param file_type 需要匹配的文件类型
		* @return 返回匹配到的文件列表
		*/
	def subDir(dir: File, file_type: String): Iterator[File] = {
		val dirs = dir.listFiles().filter(_.isDirectory())
		val files = dir.listFiles().filter(_.isFile()).filter(_.getName.endsWith("." + file_type))
		files.toIterator ++ dirs.toIterator.flatMap(subDir(_, file_type))
	}


	/**
		* 给出指定的文件,如果存在,就删除
		*
		* @param path 需要删除的文件路径
		*/
	def deleteFile(path: String): Unit = {
		val file = new File(path)
		if (file.exists()) {
			file.delete()
			//			println("\tdelete old file success : " + file.getName)
		}
	}


	/**
		* 将指定的内容写入指定的文件中
		*
		* @param strBuilder 需要写入的内容
		* @param file       需要写入的文件
		*/
	def writeBuilder(strBuilder: StringBuilder, file: String): Unit = {
		val outFileWriter = new FileWriter(file)
		outFileWriter.write(strBuilder.toString())
		outFileWriter.close()
	}


	/**
		* 删除最后一行的逗号
		*
		* @param builder
		*/
	def deleteLastComma(builder: StringBuilder): Unit = {
		try {
			builder.deleteCharAt(builder.lastIndexOf(","))
		} catch {
			case e: Exception => println("delete comma exception,can not find comma")
		}
	}
}
