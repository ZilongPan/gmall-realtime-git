package com.sleepy.gmall.realtime.util

import java.time.{Duration, Instant, LocalDate, LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

object DateUtil {
    //日期+时间 格式
    val DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss"
    //日期 格式
    val DATE_PATTERN = "yyyy-MM-dd"
    //日期+时间 格式化器
    val formatterDT: DateTimeFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN)
    //日期 格式化器
    val formatterD: DateTimeFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN)
    /**
     * 获取当前指定格式的时间 (字符串格式)
     *
     * @param pattern : 指定日期格式
     */
    def nowDateTimeStr(pattern: String = DATE_TIME_PATTERN): String = {
        val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
        val ldt: LocalDateTime = LocalDateTime.now
        ldt.format(dtf)
    }

    /**
     * 获取当前时间 字符串格式
     */
    def nowDateTimeStr: String = nowDateTimeStr()

    /**
     * 获取当前日期 字符串格式
     */
    def nowDateStr: String = nowDateTimeStr(DATE_PATTERN)

    /**
     * Long类型时间戳 ---> LocalDateTime
     *
     * @param ts : 时间戳
     */
    def tsToldt(ts: Long): LocalDateTime = {
        val tsInstant: Instant = Instant.ofEpochMilli(ts)
        LocalDateTime.ofInstant(tsInstant, ZoneId.systemDefault)
    }

    /**
     * str类型日期 ---> LocalDateTime
     *
     * @param strDate : 字符串类型的日期
     */
    def strToldt(strDate: String): LocalDateTime = {
        val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN)
        LocalDateTime.parse(strDate, dtf)
    }

    /**
     * str类型日期 ---> Long类型时间戳
     *
     * @param strDate : 字符串类型的日期
     */
    def strTots(strDate: String): Long = strToldt(strDate).toInstant(ZoneOffset.of("+8")).toEpochMilli

    /**
     * //求两个str类型的日志 相差的秒数
     *
     * @param strDate1 : 字符串类型的日期
     * @param strDate2 : 字符串类型的日期
     */
    def durationSeconds(strDate1: String, strDate2: String): Long = {
        val date1: LocalDateTime = strToldt(strDate1)
        val date2: LocalDateTime = strToldt(strDate2)
        Duration.between(date1, date2).getSeconds
    }

    /**
     * LocalDateTime--->str类型"yyyy-MM-dd"
     *
     * @param ldt : LocalDateTime类型的日期
     */
    def ldtToDate(ldt: LocalDateTime): String = {
        ldt.format(formatterD)
    }

    def ldToDate(ld: LocalDate): String = {
        ld.format(formatterD)
    }

    def main(args: Array[String]): Unit = {

    }
}