# analytics_daily_etl_queries.py
"""
SQL queries for analytics_daily_etl dag
"""

# ==========================================================================
# Queries for extract
# ==========================================================================

QUERY_FACT_DAILY_FEED_EXTRACT = """
    WITH user_actions as (
        SELECT
            user_id
            , post_id
            , action
            , time
            , lagInFrame(time) OVER (PARTITION BY  user_id, toDate(time), action ORDER BY time) prev_time
            , minIf(time, action = 'view') OVER (PARTITION BY  user_id, toDate(time), post_id) first_view_in_day
            , minIf(time, action = 'like') OVER (PARTITION BY  user_id, toDate(time), post_id) first_like_in_day
        FROM 
            feed_actions 
        WHERE 
            toDate(time) = yesterday()    
    )
    SELECT
        toDate(time) as date
        , user_id
        , minIf(time, action = 'view') as first_view_time
        , minIf(time, action = 'like') as first_like_time
        , maxIf(time, action = 'view') as last_view_time
        , maxIf(time, action = 'like') as last_like_time    
        , uniq(post_id) as posts
        , countIf(action = 'view') as views
        , countIf(action = 'like') as likes
        , avgIf(time - prev_time, action = 'view' and prev_time != toDateTime(0)) as avg_time_between_views
        , avgIf(time - prev_time, action = 'like' and prev_time != toDateTime(0)) as avg_time_between_likes
        , avgIf(
            first_like_in_day - first_view_in_day
            , first_like_in_day != toDateTime(0) 
            and first_view_in_day != toDateTime(0) 
            and first_like_in_day >= first_view_in_day
        ) as avg_view_to_like_seconds    
    FROM 
        user_actions
    GROUP BY
        date
        , user_id
    ORDER BY 
        date
        , user_id     
"""

QUERY_FACT_DAILY_POSTS_EXTRACT = """
    WITH post_actions as (
        SELECT
            user_id
            , post_id
            , action
            , time
            , lagInFrame(time) OVER (PARTITION BY  post_id, toDate(time), action ORDER BY time) prev_time
            , minIf(time, action = 'view') OVER (PARTITION BY toDate(time), post_id) first_view_in_day
            , minIf(time, action = 'like') OVER (PARTITION BY toDate(time), post_id) first_like_in_day
        FROM 
            feed_actions 
        WHERE 
            toDate(time) = yesterday()    
    )
    SELECT
        toDate(time) as date
        , post_id
        , min(time) as first_action_time
        , max(time) as last_action_time
        , countIf(action = 'view') as views
        , countIf(action = 'like') as likes
        , uniqIf(user_id, action = 'view') as unique_viewers
        , uniqIf(user_id, action = 'like') as unique_likers
        , avgIf(time - prev_time, action = 'view' and prev_time != toDateTime(0)) as avg_time_between_views
        , avgIf(time - prev_time, action = 'like' and prev_time != toDateTime(0)) as avg_time_between_likes
        , avgIf(
            first_like_in_day - first_view_in_day
            , first_like_in_day != toDateTime(0) 
            and first_view_in_day != toDateTime(0) 
            and first_like_in_day >= first_view_in_day
        ) as avg_view_to_like_seconds    
    FROM 
        post_actions
    GROUP BY
        date
        , post_id
    ORDER BY 
        date
        , post_id           
"""    

QUERY_FACT_DAILY_MESSENGER_EXTRACT = """
    WITH user_actions as (
        SELECT 
            time
            , user_id
            , receiver_id 
            , lagInFrame(time) OVER (PARTITION BY  toDate(time), user_id ORDER BY time) user_prev_time
            , lagInFrame(time) OVER (PARTITION BY  toDate(time), receiver_id ORDER BY time) receiver_prev_time
        FROM 
            message_actions 
        WHERE 
            toDate(time) = yesterday()    
        )
        , senders as (
        SELECT
            toDate(time) as date
            , user_id 
            , min(time) as first_sent_time
            , max(time) as last_sent_time    
            , uniq(receiver_id) as users_sent
            , count() as messages_sent
            , avgIf(time - user_prev_time, user_prev_time != toDateTime(0)) avg_time_between_messages_sent
        FROM 
            user_actions   
        GROUP BY 
            date
            , user_id
        )
    , receivers as (
        SELECT
            toDate(time) as date
            , receiver_id as user_id 
            , min(time) as first_received_time
            , max(time) as last_received_time      
            , uniq(user_id) as users_received
            , count() as messages_received
            , avgIf(time - receiver_prev_time, receiver_prev_time != toDateTime(0)) avg_time_between_messages_received
        FROM 
            user_actions  
        GROUP BY 
            date
            , receiver_id
    )
    SELECT 
        if(s.user_id != 0, s.date, r.date) as date
        , if(s.user_id != 0, s.user_id, r.user_id) as user_id
        , s.first_sent_time
        , s.last_sent_time
        , s.users_sent
        , s.messages_sent
        , s.avg_time_between_messages_sent
        , r.first_received_time
        , r.last_received_time
        , r.users_received
        , r.messages_received
        , r.avg_time_between_messages_received  
    FROM 
        senders s
        FULL JOIN receivers r ON s.date = r.date AND s.user_id = r.user_id
    ORDER BY 
        date
        , user_id            
"""    

QUERY_FACT_USER_CONNECTIONS_EXTRACT = """
    SELECT 
        toDate(time) as date
        , user_id as sender_id
        , receiver_id
        , count() as messages_count
    FROM 
        message_actions
    WHERE 
        toDate(time) = yesterday()  
    GROUP BY 
        date
        , sender_id
        , receiver_id
    ORDER BY 
        date
        , sender_id
        , receiver_id         
"""  

QUERY_DIM_USERS_EXTRACT = """
    SELECT 
        user_id
        , argMax(gender, time) as gender
        , argMax(age, time) as age
        , argMax(source, time) as source
        , argMax(os, time) as os
        , argMax(city, time) as city
        , argMax(country, time) as country
        , argMax(exp_group, time) as exp_group
        , toDate(now()) as version
    FROM (
        SELECT
            time
            , user_id
            , gender
            , age
            , source
            , os 
            , city 
            , country
            , exp_group 
        FROM 
            feed_actions 
        UNION ALL
        SELECT
            time
            , user_id
            , gender
            , age
            , source
            , os 
            , city 
            , country
            , exp_group       
        FROM 
            message_actions    
    )
    WHERE 
        toDate(time) = yesterday()
    GROUP BY 
        user_id
    ORDER BY 
        user_id     
"""  

# ==========================================================================
# Queries for load
# ==========================================================================

QUERY_FACT_DAILY_FEED_LOAD = """
    INSERT INTO fact_daily_feed VALUES    
"""

QUERY_FACT_DAILY_POSTS_LOAD = """
    INSERT INTO fact_daily_posts VALUES      
"""    

QUERY_FACT_DAILY_MESSENGER_LOAD = """
    INSERT INTO fact_daily_messenger VALUES        
"""    

QUERY_FACT_USER_CONNECTIONS_LOAD = """
    INSERT INTO fact_user_connections VALUES
"""  

QUERY_DIM_USERS_LOAD = """
    INSERT INTO dim_users VALUES 
"""  