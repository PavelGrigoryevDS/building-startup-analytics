"""
SQL queries for application report
"""

QUERY_DAU = """
    WITH union_data as (
        SELECT 
            toDate(time) as date
            , user_id 
            , max(service = 'feed') as has_feed
            , max(service = 'messenger') as has_messenger
        FROM (
            SELECT
                time
                , user_id
                , 'feed' as service
            FROM 
                simulator_20250820.feed_actions
            UNION ALL
            SELECT
                time
                , user_id
                , 'messenger' as service
            FROM 
                simulator_20250820.message_actions 
        )
        WHERE 
            toDate(time) BETWEEN yesterday() - 13 AND yesterday()  
        GROUP BY 
            date
            , user_id 
    )
    SELECT 
        date
        , uniq(user_id) as total_dau
        , uniqIf(user_id, has_feed = 1 and has_messenger = 0) as feed_only_dau
        , uniqIf(user_id, has_feed = 0 and has_messenger = 1) as messenger_only_dau
        , uniqIf(user_id, has_feed = 1 and has_messenger = 1) as both_services_dau
        , if(date >= yesterday() - 6, 'current', 'previous') as week_status
    FROM 
        union_data
    GROUP BY 
        date
    ORDER BY 
        date    
"""        

QUERY_NEW_USERS = """
    WITH first_actions AS (
        SELECT 
            user_id
            , toDate(min(time)) as first_date
            , toDate(minIf(time, service = 'feed')) as first_feed_date
            , toDate(minIf(time, service = 'messenger')) as first_messenger_date
        FROM (
            SELECT time, user_id, 'feed' as service
            FROM simulator_20250820.feed_actions
            UNION ALL
            SELECT time, user_id, 'messenger' as service  
            FROM simulator_20250820.message_actions
        )
        GROUP BY user_id
    )
    , first_all as (
        SELECT 
        first_date as date
        , uniq(user_id) as all_new_users
        FROM 
        first_actions
        WHERE 
        date BETWEEN yesterday() - 13 AND yesterday()
        GROUP BY 
        date
    )
    , first_feed as (
        SELECT 
        first_feed_date as date
        , uniq(user_id) as feed_new_users
        FROM 
        first_actions
        WHERE 
        date BETWEEN yesterday() - 13 AND yesterday()      
        GROUP BY 
        date
    )
    , first_messenger as (
        SELECT 
        first_messenger_date as date
        , uniq(user_id) as messenger_new_users
        FROM 
        first_actions
        WHERE 
        date BETWEEN yesterday() - 13 AND yesterday()      
        GROUP BY 
        date
    )
    , both_activity as (
        SELECT 
        toDate(time) as date
        , user_id
        FROM (
            SELECT time, user_id, 'feed' as service
            FROM simulator_20250820.feed_actions
            UNION ALL
            SELECT time, user_id, 'messenger' as service  
            FROM simulator_20250820.message_actions
        )
        GROUP BY 
        date
        , user_id
        HAVING 
        uniq(service) = 2
    )
    , first_both as (
        SELECT 
        date
        , uniq(user_id) as both_first_users
        FROM 
        both_activity
        WHERE 
        date BETWEEN yesterday() - 13 AND yesterday()      
        GROUP BY 
        date
    )
    SELECT 
        a.date as date
        , if(date >= yesterday() - 6, 'current', 'previous') as week_status
        , ifNull(a.all_new_users, 0) as all_new_users
        , ifNull(f.feed_new_users, 0) as feed_new_users
        , ifNull(m.messenger_new_users, 0) as messenger_new_users
        , ifNull(b.both_first_users, 0) as both_first_users
    FROM 
        first_all a
    LEFT JOIN first_feed f ON a.date = f.date
    LEFT JOIN first_messenger m ON a.date = m.date
    LEFT JOIN first_both b ON a.date = b.date
"""

QUERY_ACTIVITY = """
    WITH feed_stats as (
        SELECT 
            toDate(time) as date
            , countIf(action = 'view') as views
            , countIf(action = 'like') as likes
            , ifNull(likes / nullIf(views, 0), 0) as ctr
        FROM 
            simulator_20250820.feed_actions
        WHERE 
            date BETWEEN yesterday() - 13 AND yesterday()
        GROUP BY 
            date
    )
    , messenger_stats as (
        SELECT 
            toDate(time) as date
            , count() as messages
        FROM 
            simulator_20250820.message_actions 
        WHERE 
            date BETWEEN yesterday() - 13 AND yesterday()
        GROUP BY 
            date
    )    
    SELECT 
        if(f.date != toDate(0), f.date, m.date) as date
        , if(date >= yesterday() - 6, 'current', 'previous') as week_status
        , ifNull(f.views, 0) as views
        , ifNull(f.likes, 0) as likes
        , ifNull(f.ctr, 0) as ctr
        , ifNull(m.messages, 0) as messages
    FROM 
        feed_stats f
        FULL JOIN messenger_stats m ON f.date = m.date
    ORDER BY 
        date
"""    

QUERY_RETENTION = """
    WITH cohorts as (
        SELECT 
            user_id
            , toDate(min(time)) as cohort
        FROM (
            SELECT
                time
                , user_id
            FROM 
                simulator_20250820.feed_actions
            UNION ALL 
            SELECT
                time
                , user_id
            FROM 
                simulator_20250820.message_actions 
        )
        GROUP BY 
            user_id 
        HAVING  
            cohort BETWEEN yesterday() - 14 AND yesterday() - 7
    )
    , activity as (
        SELECT 
            toDate(time) as date
            , user_id
        FROM (
            SELECT
                time
                , user_id
            FROM 
                simulator_20250820.feed_actions
            UNION ALL  
            SELECT
                time
                , user_id
            FROM 
                simulator_20250820.message_actions 
            )
        WHERE   
            date BETWEEN yesterday() - 14 AND yesterday()
        GROUP BY 
            date
            , user_id
    )
    SELECT 
        c.cohort
        , a.date - c.cohort as lifetime
        , uniq(user_id) as users
    FROM 
        cohorts c
        JOIN activity a ON c.user_id = a.user_id
    WHERE 
        lifetime <= 7
    GROUP BY 
        c.cohort
        , lifetime
    ORDER BY 
        c.cohort  
"""

QUERY_ROLL_RETENTION_7D = """
    WITH users as (
        SELECT 
            user_id
            , max(toDate(time) = yesterday()) as returned_yesterday
            , max(toDate(time) != yesterday()) as returned_prev_week
        FROM (
            SELECT
                time
                , user_id
            FROM 
                simulator_20250820.feed_actions
            UNION ALL 
            SELECT
                time
                , user_id
            FROM 
                simulator_20250820.message_actions 
        )
        WHERE 
            toDate(time) BETWEEN yesterday() - 7 AND yesterday()
        GROUP BY 
            user_id
    )
    SELECT 
        sum(returned_yesterday) as yesterday_users
        , sum(returned_yesterday AND returned_prev_week) as all_week_users
        , all_week_users / yesterday_users as retention_7d
    FROM 
        users
"""

QUERY_FEED_DETAILED = """
    WITH users_stats as (
        SELECT 
            toDate(time) as date
            , user_id
            , uniq(post_id) as posts
            , countIf(action = 'view') as views
            , countIf(action = 'like') as likes
            , ifNull(likes / nullIf(views, 0), 0) as ctr
        FROM 
            simulator_20250820.feed_actions
        WHERE 
            toDate(time) BETWEEN yesterday() - 13 AND yesterday()
        GROUP BY 
            date
            , user_id
    )
    SELECT 
        date
        , if(date >= yesterday() - 6, 'current', 'previous') as week_status
        , AVG(posts) as posts_per_user
        , AVG(views) as views_per_user
        , AVG(likes) as likes_per_user
        , AVG(ctr) as ctr_per_user
    FROM 
        users_stats
    GROUP BY 
        date
"""

QUERY_MESSENGER_DETAILED = """
    SELECT 
        toDate(time) as date
        , if(date >= yesterday() - 6, 'current', 'previous') as week_status
        , uniq(user_id) as sender_dau
        , uniq(receiver_id) as receiver_dau
        , ifNull(sender_dau / nullIf(receiver_dau, 0), 0) as sender_to_receiver_ratio
        , ifNull(count() / nullIf(sender_dau, 0), 0) as messages_per_sender
    FROM 
        simulator_20250820.message_actions
    WHERE 
        toDate(time) BETWEEN yesterday() - 13 AND yesterday()
    GROUP BY 
        date
    ORDER BY 
        date        
"""

QUERY_USERS_DAILY_BY_SOURCE = """
    WITH union_data as (
        SELECT 
            toDate(time) as date
            , user_id 
            , max(service = 'feed') as has_feed
            , max(service = 'messenger') as has_messenger
            , any(source) as source
        FROM (
            SELECT
                time
                , user_id
                , 'feed' as service
                , source
            FROM 
                simulator_20250820.feed_actions
            UNION ALL
            SELECT
                time
                , user_id
                , 'messenger' as service
                , source
            FROM 
                simulator_20250820.message_actions 
        )
        WHERE 
            toDate(time) BETWEEN yesterday() - 6 AND yesterday()  
        GROUP BY 
            date
            , user_id 
    )
    SELECT 
        date
        , source
        , uniq(user_id) as total_dau
        , uniqIf(user_id, has_feed = 1 and has_messenger = 0) as feed_only_dau
        , uniqIf(user_id, has_feed = 0 and has_messenger = 1) as messenger_only_dau
        , uniqIf(user_id, has_feed = 1 and has_messenger = 1) as both_services_dau
    FROM 
        union_data
    GROUP BY 
        date
        , source
    ORDER BY 
        date   
"""        