package dmp.Utils

object SqlStatementUtils {

  val locationSql =
    """
        select
          provincename,
          cityname,

          sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) origin_request_count,
          sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) effective_request_count,
          sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) ad_request_count,

          sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) join_bid_count,
          sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) bid_success_count,

          sum(case when requestmode = 2 and isbilling = 1 and iseffective = 1 then 1 else 0 end) show_count,
          sum(case when requestmode = 3 and isbilling = 1 and iseffective = 1 then 1 else 0 end) click_count,

          sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice else 0 end)/1000 dsp_ad_consume,
          sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment else 0 end)/1000 dsp_ad_cost

        from logs
        group by provincename, cityname
    """

  val mdediaAnalysisSql =
    """
        select
          appname,

          sum(origin_request) origin_request_count,
          sum(effective_request) effective_request_count,
          sum(ad_request) ad_request_count,

          sum(join_bid) join_bid_count,
          sum(bid_success) bid_success_count,

          sum(show) show_count,
          sum(click) click_count,

          sum(ad_consume)/1000 dsp_ad_consume,
          sum(ad_cost)/1000 dsp_ad_cost

        from(
          select
            appid,
         -- if(appname = '其他', appname2, appname) appname,
            case
              when appname = '其他' and appname2 is not null then appname2
              when appname = '其他' and appname2 is null then 'dict中不存在对应的appname'
              else appname
            end appname,

            origin_request,
            effective_request,
            ad_request,

            join_bid,
            bid_success,

            show,
            click,

            ad_consume,
            ad_cost

          from(
            select
              tmp.appid appid,
              tmp.appname appname,
              appid_appname_info.appname appname2,

              origin_request,
              effective_request,
              ad_request,

              join_bid,
              bid_success,

              show,
              click,

              ad_consume,
              ad_cost

            from(
              select
                appid,
                appname,

                case when requestmode = 1 and processnode >= 1 then 1 else 0 end origin_request,
                case when requestmode = 1 and processnode >= 2 then 1 else 0 end effective_request,
                case when requestmode = 1 and processnode = 3 then 1 else 0 end ad_request,

                case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end join_bid,
                case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end bid_success,

                case when requestmode = 2 and isbilling = 1 and iseffective = 1 then 1 else 0 end show,
                case when requestmode = 3 and isbilling = 1 and iseffective = 1 then 1 else 0 end click,

                case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice else 0 end ad_consume,
                case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment else 0 end ad_cost

              from logs
            ) tmp left join appid_appname_info on tmp.appid = appid_appname_info.appid
          ) tmp2
        ) tmp3
        where appname = '爱奇艺' or appname = '腾讯新闻' or appname = 'PPTV'
        group by appname
    """
}
