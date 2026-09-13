-- =========================================================
-- LOAD DATA
-- =========================================================

users = LOAD '/bdaproject/Users.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(
    ',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER'
)
AS (
    AboutMe:chararray,
    AccountId:long,
    CreationDate:chararray,
    DisplayName:chararray,
    DownVotes:int,
    Id:long,
    LastAccessDate:chararray,
    Location:chararray,
    Reputation:int,
    UpVotes:int,
    Views:long,
    WebsiteUrl:chararray
);

posts = LOAD '/bdaproject/Posts.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(
    ',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER'
)
AS (
    AcceptedAnswerId:long,
    AnswerCount:int,
    Body:chararray,
    ClosedDate:chararray,
    CommentCount:int,
    CommunityOwnedDate:chararray,
    ContentLicense:chararray,
    CreationDate:chararray,
    Id:long,
    LastActivityDate:chararray,
    LastEditDate:chararray,
    LastEditorDisplayName:chararray,
    LastEditorUserId:long,
    OwnerDisplayName:chararray,
    OwnerUserId:long,
    ParentId:long,
    PostTypeId:int,
    Score:int,
    Tags:chararray,
    Title:chararray,
    ViewCount:long
);

comments = LOAD '/bdaproject/Comments.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(
    ',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER'
)
AS (
    ContentLicense:chararray,
    CreationDate:chararray,
    Id:long,
    PostId:long,
    Score:int,
    Text:chararray,
    UserDisplayName:chararray,
    UserId:long
);

tags = LOAD '/bdaproject/Tags.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(
    ',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER'
)
AS (
    Count:int,
    ExcerptPostId:long,
    Id:long,
    TagName:chararray,
    WikiPostId:long
);


-- =========================================================
-- 1. USERS WITH REPUTATION GREATER THAN 1000
-- =========================================================

high_reputation_users = FILTER users BY Reputation > 1000;

high_reputation_result = FOREACH high_reputation_users
GENERATE
    CONCAT(
        (chararray)Id,
        CONCAT(
            ' | ',
            CONCAT(
                DisplayName,
                CONCAT(
                    ' | ',
                    (chararray)Reputation
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 2. USERS WITH WEBSITE
-- =========================================================

website_users = FILTER users BY WebsiteUrl IS NOT NULL
                AND WebsiteUrl != '';

website_result = FOREACH website_users
GENERATE
    CONCAT(
        (chararray)Id,
        CONCAT(
            ' | ',
            CONCAT(
                DisplayName,
                CONCAT(
                    ' | ',
                    WebsiteUrl
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 3. DISTINCT LOCATIONS
-- =========================================================

locations = FILTER users BY Location IS NOT NULL
            AND Location != '';

grouped_locations = GROUP locations BY Location;

distinct_location_result = FOREACH grouped_locations
GENERATE
    CONCAT(
        (chararray)group,
        CONCAT(
            ' | ',
            (chararray)COUNT(locations)
        )
    ) AS Result;


-- =========================================================
-- 4. TOP 10 USERS BY REPUTATION
-- =========================================================

sorted_users = ORDER users BY Reputation DESC;

top10_users = LIMIT sorted_users 10;

top10_users_result = FOREACH top10_users
GENERATE
    CONCAT(
        (chararray)Id,
        CONCAT(
            ' | ',
            CONCAT(
                DisplayName,
                CONCAT(
                    ' | ',
                    (chararray)Reputation
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 5. POSTS WITH SCORE GREATER THAN 10
-- =========================================================

high_score_posts = FILTER posts BY Score > 10;

high_score_result = FOREACH high_score_posts
GENERATE
    CONCAT(
        (chararray)Id,
        CONCAT(
            ' | ',
            CONCAT(
                (chararray)Score,
                CONCAT(
                    ' | ',
                    Title
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 6. TOP 10 POSTS BY SCORE
-- =========================================================

sorted_posts_score = ORDER posts BY Score DESC;

top10_posts = LIMIT sorted_posts_score 10;

top10_posts_result = FOREACH top10_posts
GENERATE
    CONCAT(
        (chararray)Id,
        CONCAT(
            ' | ',
            CONCAT(
                (chararray)Score,
                CONCAT(
                    ' | ',
                    Title
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 7. TOP 10 VIEWED POSTS
-- =========================================================

sorted_posts_views = ORDER posts BY ViewCount DESC;

top10_views = LIMIT sorted_posts_views 10;

top10_views_result = FOREACH top10_views
GENERATE
    CONCAT(
        (chararray)Id,
        CONCAT(
            ' | ',
            CONCAT(
                (chararray)ViewCount,
                CONCAT(
                    ' | ',
                    Title
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 8. POSTS WITH MORE THAN 10 COMMENTS
-- =========================================================

popular_posts = FILTER posts BY CommentCount > 10;

popular_posts_result = FOREACH popular_posts
GENERATE
    CONCAT(
        (chararray)Id,
        CONCAT(
            ' | ',
            CONCAT(
                (chararray)CommentCount,
                CONCAT(
                    ' | ',
                    Title
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 9. POSTS GROUPED BY POST TYPE
-- =========================================================

post_types = GROUP posts BY PostTypeId;
post_type_count = FOREACH post_types
GENERATE
    CONCAT(
        (chararray)group,
        CONCAT(
            ' | ',
            (chararray)COUNT(posts)
        )
    ) AS Result;


-- =========================================================
-- 10. JOIN USERS WITH POSTS
-- =========================================================

users_posts_join = JOIN users BY Id, posts BY OwnerUserId;
user_posts_result = FOREACH users_posts_join
GENERATE
    CONCAT(
        (chararray)users::Id,
        CONCAT(
            ' | ',
            CONCAT(
                users::DisplayName,
                CONCAT(
                    ' | ',
                    CONCAT(
                        (chararray)posts::Id,
                        CONCAT(
                            ' | ',
                            posts::Title
                        )
                    )
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 11. JOIN POSTS WITH COMMENTS
-- =========================================================

posts_comments_join = JOIN posts BY Id, comments BY PostId;

post_comments_result = FOREACH posts_comments_join
GENERATE
    CONCAT(
        (chararray)posts::Id,
        CONCAT(
            ' | ',
            CONCAT(
                (chararray)comments::Id,
                CONCAT(
                    ' | ',
                    comments::Text
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 12. JOIN USERS WITH COMMENTS
-- =========================================================

users_comments_join = JOIN users BY Id, comments BY UserId;

user_comments_result = FOREACH users_comments_join
GENERATE
    CONCAT(
        (chararray)users::Id,
        CONCAT(
            ' | ',
            CONCAT(
                users::DisplayName,
                CONCAT(
                    ' | ',
                    CONCAT(
                        (chararray)comments::Id,
                        CONCAT(
                            ' | ',
                            comments::Text
                        )
                    )
                )
            )
        )
    ) AS Result;


-- =========================================================
-- 13. COMMENTS GROUPED BY POST
-- =========================================================

comments_by_post = GROUP comments BY PostId;

comment_post_result = FOREACH comments_by_post
GENERATE
    CONCAT(
        (chararray)group,
        CONCAT(
            ' | ',
            (chararray)COUNT(comments)
        )
    ) AS Result;


-- =========================================================
-- 14. COMMENTS GROUPED BY USER
-- =========================================================

comments_by_user = GROUP comments BY UserId;

comment_user_result = FOREACH comments_by_user
GENERATE
    CONCAT(
        (chararray)group,
        CONCAT(
            ' | ',
            (chararray)COUNT(comments)
        )
    ) AS Result;


-- =========================================================
-- 15. TOP 10 USERS BY NUMBER OF COMMENTS
-- =========================================================

comment_user_count = FOREACH comments_by_user
GENERATE
    group AS UserId,
    COUNT(comments) AS CommentCount;

sorted_comment_users = ORDER comment_user_count BY CommentCount DESC;

top10_comment_users = LIMIT sorted_comment_users 10;

top10_comment_result = FOREACH top10_comment_users
GENERATE
    CONCAT(
        (chararray)UserId,
        CONCAT(
            ' | ',
            (chararray)CommentCount
        )
    ) AS Result;


-- =========================================================
-- 16. TOP 10 TAGS
-- =========================================================

sorted_tags = ORDER tags BY Count DESC;

top10_tags = LIMIT sorted_tags 10;

top10_tags_result = FOREACH top10_tags
GENERATE
    CONCAT(
        TagName,
        CONCAT(
            ' | ',
            (chararray)Count
        )
    ) AS Result;


-- =========================================================
-- 17. POPULAR TAGS WITH COUNT GREATER THAN 100
-- =========================================================

popular_tags = FILTER tags BY Count > 100;

popular_tags_result = FOREACH popular_tags
GENERATE
    CONCAT(
        TagName,
        CONCAT(
            ' | ',
            (chararray)Count
        )
    ) AS Result;


-- =========================================================
-- 18. TOP 10 USERS BY NUMBER OF POSTS
-- =========================================================

posts_by_user = GROUP posts BY OwnerUserId;

post_count_by_user = FOREACH posts_by_user
GENERATE
    group AS UserId,
    COUNT(posts) AS PostCount;

sorted_post_users = ORDER post_count_by_user BY PostCount DESC;

top10_post_users = LIMIT sorted_post_users 10;

top10_post_users_result = FOREACH top10_post_users
GENERATE
    CONCAT(
        (chararray)UserId,
        CONCAT(
            ' | ',
            (chararray)PostCount
        )
    ) AS Result;


-- =========================================================
-- STORE RESULTS
-- =========================================================

STORE high_reputation_result
INTO '/PigOutPut/01_HighReputationUsers'
USING PigStorage('|');

STORE website_result
INTO '/PigOutPut/02_UsersWithWebsite'
USING PigStorage('|');

STORE distinct_location_result
INTO '/PigOutPut/03_DistinctLocations'
USING PigStorage('|');

STORE top10_users_result
INTO '/PigOutPut/04_Top10UsersByReputation'
USING PigStorage('|');

STORE high_score_result
INTO '/PigOutPut/05_HighScorePosts'
USING PigStorage('|');

STORE top10_posts_result
INTO '/PigOutPut/06_Top10PostsByScore'
USING PigStorage('|');

STORE top10_views_result
INTO '/PigOutPut/07_Top10ViewedPosts'
USING PigStorage('|');

STORE popular_posts_result
INTO '/PigOutPut/08_PostsMoreThan10Comments'
USING PigStorage('|');

STORE post_type_count
INTO '/PigOutPut/09_PostsByPostType'
USING PigStorage('|');

STORE user_posts_result
INTO '/PigOutPut/10_UsersJoinPosts'
USING PigStorage('|');

STORE post_comments_result
INTO '/PigOutPut/11_PostsJoinComments'
USING PigStorage('|');

STORE user_comments_result
INTO '/PigOutPut/12_UsersJoinComments'
USING PigStorage('|');

STORE comment_post_result
INTO '/PigOutPut/13_CommentsGroupedByPost'
USING PigStorage('|');

STORE comment_user_result
INTO '/PigOutPut/14_CommentsGroupedByUser'
USING PigStorage('|');

STORE top10_comment_result
INTO '/PigOutPut/15_Top10CommentUsers'
USING PigStorage('|');

STORE top10_tags_result
INTO '/PigOutPut/16_Top10Tags'
USING PigStorage('|');

STORE popular_tags_result
INTO '/PigOutPut/17_PopularTags'
USING PigStorage('|');

STORE top10_post_users_result
INTO '/PigOutPut/18_Top10UsersByPosts'
USING PigStorage('|');