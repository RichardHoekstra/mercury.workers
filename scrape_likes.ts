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
const REFRESH_THRESHOLD_IN_MS = 8 * 60 * 60 * 1000; // Every 12 hours
const twitterClient = new TwitterApi(process.env.TWITTER_API_BEARER || "");
const twtr = twitterClient.readOnly;

let SLEEP_UNTIL_TIMESTAMP_IN_MS = 0;

const loop = async () => {
    // Fetch stale user
    // Fetch likes from database
    // Fetch likes from twitter API
    // Compare database and twitter API
    // Write changes to database
    const staleUser = await prisma.tUser.findFirst({
        where: {
            marked: true,
            accountExists: true,
            twitterMetaData: {
                protected: false,
            },
        },
        orderBy: [{ likesScrapedAt: "asc" }],
    });

    console.log(
        `${new Date().toISOString()}\t${
            staleUser?.username
        }\t${staleUser?.likesScrapedAt.toISOString()}`
    );
    if (!staleUser) return;
    if (
        !(
            Date.now() - staleUser.likesScrapedAt.getTime() >
            REFRESH_THRESHOLD_IN_MS
        )
    ) {
        SLEEP_UNTIL_TIMESTAMP_IN_MS =
            staleUser.likesScrapedAt.getTime() + REFRESH_THRESHOLD_IN_MS;
        SLEEP_TIME_MS *= SLEEP_MULTIPLIER_ON_ERROR;
        console.log(
            `${new Date().toISOString()}\tSLEEP UNTIL ${new Date(
                SLEEP_UNTIL_TIMESTAMP_IN_MS / 1000.0
            ).toISOString()}`
        );
        return;
    }

    const dbLikesIds = (
        await prisma.tLike.findMany({
            where: {
                tUserId: staleUser.id,
            },
            orderBy: {
                recordCreatedAt: "desc",
            },
            take: 1000,
            select: {
                tTweetId: true,
            },
        })
    ).map((like) => like.tTweetId);

    const request = await twtr.v2.userLikedTweets(staleUser.id, {
        max_results: 100,
        "tweet.fields": [
            "attachments",
            "author_id",
            "context_annotations",
            "conversation_id",
            "created_at",
            "entities",
            "geo",
            "id",
            "in_reply_to_user_id",
            "lang",
            "public_metrics",
            "possibly_sensitive",
            "referenced_tweets",
            "reply_settings",
            "source",
            "text",
            "withheld",
        ],
    });
    const twtrLikes = request.tweets;
    const twtrLikesIds = twtrLikes.map((like) => like.id);

    const _old = new Set(dbLikesIds);
    const _new = new Set(twtrLikesIds);
    const added = new Set([..._new].filter((x) => !_old.has(x)));
    for (const id of added) {
        await writeChangesToDb(staleUser.id, twtrLikes, id);
    }

    await prisma.tUser.update({
        where: { id: staleUser.id },
        data: { likesScrapedAt: new Date() },
    });
    console.log(
        `${new Date().toISOString()}\tLikes: ${dbLikesIds.length} Added: ${
            added.size
        }\t`
    );
};

export const scraper = async () => {
    let EXECUTION_TIME_MS = 0;
    while (true) {
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
                    console.log(
                        `${new Date().toISOString()}\tRatelimited: ${error}\t`
                    );
                    SLEEP_UNTIL_TIMESTAMP_IN_MS =
                        error.rateLimit.reset * 1000.0;
                    SLEEP_TIME_MS *= SLEEP_MULTIPLIER_ON_ERROR;
                } else {
                    throw error;
                }
            }
        }
        const sleepTime = Math.max(1000, SLEEP_TIME_MS - EXECUTION_TIME_MS);
        console.log(
            "\tEXEC_TIME:",
            (EXECUTION_TIME_MS / 1000).toLocaleString(),
            "s"
        );
        console.log("\tSLEEP_TIME:", (sleepTime / 1000).toLocaleString(), "s");
        await sleep(sleepTime);
    }
};

scraper()
    .then()
    .catch((e) => {
        console.log(e);
        throw e;
    });
