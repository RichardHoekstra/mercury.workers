import * as dotenv from "dotenv";
import {
    TwitterApi,
    TwitterRateLimit,
    ApiResponseError,
    UserV2,
    ApiRequestError,
    TweetV2,
} from "twitter-api-v2";
import { PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

function log(message: string) {
    console.log("PROFILES\t" + message);
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function upsertTweet(newTweet: TweetV2) {
    const twitterPublicMetrics = {
        retweet_count: newTweet.public_metrics?.retweet_count,
        reply_count: newTweet.public_metrics?.reply_count,
        like_count: newTweet.public_metrics?.like_count,
        quote_count: newTweet.public_metrics?.quote_count,
    };

    const tweetCreate = {
        id: newTweet.id,
        tweetText: newTweet.text,
        createdAt: newTweet.created_at!,
        authorId: newTweet.author_id!,
        conversationId: newTweet.conversation_id!,

        referencedTweets: newTweet.referenced_tweets
            ? JSON.stringify(newTweet.referenced_tweets)
            : undefined,
        attachments: newTweet.attachments
            ? JSON.stringify(newTweet.attachments)
            : undefined,
        geo: newTweet.geo ? JSON.stringify(newTweet.geo) : undefined,
        context_annotations: newTweet.context_annotations
            ? JSON.stringify(newTweet.context_annotations)
            : undefined,
        entities: newTweet.entities
            ? JSON.stringify(newTweet.entities)
            : undefined,
        withheld: newTweet.withheld
            ? JSON.stringify(newTweet.withheld)
            : undefined,
        possibly_sensitive: newTweet.possibly_sensitive!,
        lang: newTweet.lang!,
        reply_settings: newTweet.reply_settings!,
        source: newTweet.source!,

        twitterPublicMetrics: {
            create: twitterPublicMetrics,
        },
    };

    const tweetUpsert = {
        ...tweetCreate,
        twitterPublicMetrics: {
            upsert: {
                create: twitterPublicMetrics,
                update: twitterPublicMetrics,
            },
        },
    };

    const upsertedTweet = await prisma.tTweet.upsert({
        where: { id: newTweet.id },
        create: tweetCreate,
        update: tweetUpsert,
    });

    return upsertedTweet;
}

async function writeChangesToDb(
    userId: string,
    likedTweets: TweetV2[],
    likedTweetId: string
) {
    const likedTweetInfo = likedTweets.find((x) => x.id === likedTweetId);
    const likedTweet = likedTweetInfo
        ? await upsertTweet(likedTweetInfo)
        : await prisma.tTweet.findUnique({ where: { id: likedTweetId } }); // If the info is undefined, this user is being removed
    if (!likedTweet) return;

    return await prisma.tLike.upsert({
        where: {
            tTweetId_tUserId: {
                tTweetId: likedTweet.id,
                tUserId: userId,
            },
        },
        create: {
            tUser: { connect: { id: userId } },
            tTweet: { connect: { id: likedTweet.id } },
        },
        update: {},
    });
}

dotenv.config();
let SLEEP_TIME_MS = 60 * 1_000;
const SLEEP_MULTIPLIER_ON_ERROR = 1.1;
const REFRESH_THRESHOLD_IN_MS = 8 * 60 * 60 * 1000; // Every 8 hours
const twitterClient = new TwitterApi(process.env.TWITTER_API_BEARER || "");
const twtr = twitterClient.readOnly;

let SLEEP_UNTIL_TIMESTAMP_IN_MS = 0;

const loop = async () => {
    // Fetch a list of stale users
    // Fetch likes from database
    // Fetch likes from twitter API
    // Compare database and twitter API
    // Write changes to database
    const staleUsers = await prisma.tUser.findMany({
        where: {
            AND: {
                marked: true,
                accountExists: true,
            },
        },
        orderBy: [{ profileScrapedAt: "asc" }],
        take: 100,
    });
    log(
        `${new Date().toISOString()}\tUSERS(${staleUsers.length}): [${
            staleUsers[0].username
        }]...[${
            staleUsers[staleUsers.length - 1].username
        }]\t${staleUsers[0]?.likesScrapedAt.toISOString()}`
    );
    if (!staleUsers || staleUsers.length === 0) return;
    if (
        !(
            Date.now() - staleUsers[0].likesScrapedAt.getTime() >
            REFRESH_THRESHOLD_IN_MS
        )
    ) {
        SLEEP_UNTIL_TIMESTAMP_IN_MS =
            staleUsers[0].likesScrapedAt.getTime() + REFRESH_THRESHOLD_IN_MS;
        SLEEP_TIME_MS *= SLEEP_MULTIPLIER_ON_ERROR;
        log(
            `${new Date().toISOString()}\tSLEEP UNTIL ${new Date(
                SLEEP_UNTIL_TIMESTAMP_IN_MS
            ).toISOString()}`
        );
        return;
    }

    // Fetch profiles of these 100 marked users
    const userIds = staleUsers.map((user) => user.id);
    const request = await twtr.v2.users(userIds, {
        "user.fields": [
            "created_at",
            "description",
            "entities",
            "id",
            "location",
            "name",
            "pinned_tweet_id",
            "profile_image_url",
            "protected",
            "public_metrics",
            "url",
            "username",
            "verified",
            "withheld",
        ],
        expansions: "pinned_tweet_id",
    });

    // If request does not include these ids,
    // the user no longer exists.
    const requestIds = request.data.map((user) => user.id);
    const removedIds = userIds.filter((id) => !requestIds.includes(id));
    for (const id of removedIds) {
        await prisma.tUser.update({
            where: {
                id: id,
            },
            data: { accountExists: false },
        });
    }
    log(`${new Date().toISOString()}\tRemoved: ${removedIds.length}`);

    // Update profiles for these 100 marked users in the database
    for (const user of request.data) {
        const twitterMetaData = {
            createdAt: user.created_at,
            description: user.description,
            entities: user.entities ? JSON.stringify(user.entities) : undefined,
            location: user.location,
            pinned_tweet_id: user.pinned_tweet_id,
            profile_image_url: user.profile_image_url,
            protected: user.verified,
            url: user.url,
            verified: user.verified,
        };

        const twitterPublicMetrics = {
            followers_count: user.public_metrics?.followers_count,
            following_count: user.public_metrics?.following_count,
            tweet_count: user.public_metrics?.tweet_count,
            listed_count: user.public_metrics?.listed_count,
        };

        await prisma.tUser.update({
            where: { id: user.id },
            data: {
                profileScrapedAt: new Date(),
                twitterMetaData: {
                    upsert: {
                        update: twitterMetaData,
                        create: twitterMetaData,
                    },
                },
                twitterPublicMetrics: {
                    upsert: {
                        update: twitterPublicMetrics,
                        create: twitterPublicMetrics,
                    },
                },
            },
        });
    }
    log(`${new Date().toISOString()}\tAdded: ${requestIds.length}`);
};

export const scraperProfiles = async () => {
    let EXECUTION_TIME_MS = 0;
    let REFRESH_VIEWS_AT = 0;
    const REFRESH_VIEWS_INTERVAL_IN_MS = 8 * 60 * 60 * 1000; // 8 hours
    while (true) {
        if (Date.now() > REFRESH_VIEWS_AT) {
            log(`${new Date().toISOString()}\tREFRESHING VIEWS`);

            await prisma.$executeRaw`REFRESH MATERIALIZED VIEW public.popular_user WITH DATA;`;
            await prisma.$executeRaw`REFRESH MATERIALIZED VIEW public.trending_user_1d WITH DATA;`;
            await prisma.$executeRaw`REFRESH MATERIALIZED VIEW public.trending_user_3d WITH DATA;`;
            await prisma.$executeRaw`REFRESH MATERIALIZED VIEW public.trending_user_7d WITH DATA;`;
            await prisma.$executeRaw`REFRESH MATERIALIZED VIEW public.trending_user_30d WITH DATA;`;
            await prisma.$executeRaw`REFRESH MATERIALIZED VIEW public.trending_user_90d WITH DATA;`;
            await prisma.$executeRaw`REFRESH MATERIALIZED VIEW public.trending_user_180d WITH DATA;`;
            await prisma.$executeRaw`REFRESH MATERIALIZED VIEW public.trending_user_365d WITH DATA;`;
            log(`${new Date().toISOString()}\tDONE REFRESHING VIEWS`);
            REFRESH_VIEWS_AT = Date.now() + REFRESH_VIEWS_INTERVAL_IN_MS;
        }

        if (Date.now() > SLEEP_UNTIL_TIMESTAMP_IN_MS) {
            try {
                const start = Date.now();
                await loop();
                const end = Date.now();
                EXECUTION_TIME_MS = end - start;
            } catch (error) {
                if (
                    error instanceof ApiResponseError &&
                    error.rateLimitError &&
                    error.rateLimit
                ) {
                    log(`${new Date().toISOString()}\tRatelimited: ${error}\t`);
                    SLEEP_UNTIL_TIMESTAMP_IN_MS =
                        error.rateLimit.reset * 1000.0;
                    SLEEP_TIME_MS *= SLEEP_MULTIPLIER_ON_ERROR;
                } else {
                    throw error;
                }
            }
        }
        const sleepTime = Math.max(1000, SLEEP_TIME_MS - EXECUTION_TIME_MS);
        log(
            "\tEXEC_TIME: " + (EXECUTION_TIME_MS / 1000).toLocaleString() + "s"
        );
        log("\tSLEEP_TIME: " + (sleepTime / 1000).toLocaleString() + "s");
        await sleep(sleepTime);
    }
};

scraperProfiles()
    .then()
    .catch((e) => {
        log(e);
        throw e;
    });
