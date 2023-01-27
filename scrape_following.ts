import * as dotenv from "dotenv";
import {
    TwitterApi,
    TwitterRateLimit,
    ApiResponseError,
    UserV2,
    ApiRequestError,
} from "twitter-api-v2";
import { Prisma, PrismaClient } from "@prisma/client";
const prisma = new PrismaClient();

function log(message: string) {
    console.log("FOLLOWING\t" + message);
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function upsertUser(newUser: UserV2) {
    const twitterMetaData = {
        createdAt: newUser.created_at,
        description: newUser.description,
        entities: newUser.entities
            ? JSON.stringify(newUser.entities)
            : undefined,
        location: newUser.location,
        pinned_tweet_id: newUser.pinned_tweet_id,
        profile_image_url: newUser.profile_image_url,
        protected: newUser.verified,
        url: newUser.url,
        verified: newUser.verified,
    };

    const twitterPublicMetrics = {
        followers_count: newUser.public_metrics?.followers_count,
        following_count: newUser.public_metrics?.following_count,
        tweet_count: newUser.public_metrics?.tweet_count,
        listed_count: newUser.public_metrics?.listed_count,
    };

    const userCreate = {
        id: newUser.id,
        name: newUser.name.replace(/\0/g, ""),
        username: newUser.username,
        accountCreatedAt: newUser.created_at,
        accountExists: true,
        marked: false,
        twitterMetaData: {
            create: twitterMetaData,
        },
        twitterPublicMetrics: {
            create: twitterPublicMetrics,
        },
    };

    const userUpsert = {
        id: newUser.id,
        name: newUser.name.replace(/\0/g, ""),
        username: newUser.username,
        accountCreatedAt: newUser.created_at,
        twitterMetaData: {
            upsert: {
                create: twitterMetaData,
                update: twitterMetaData,
            },
        },
        twitterPublicMetrics: {
            upsert: {
                create: twitterPublicMetrics,
                update: twitterPublicMetrics,
            },
        },
    };

    const upsertedUser = await prisma.tUser.upsert({
        where: { id: newUser.id },
        create: userCreate,
        update: userUpsert,
    });

    return upsertedUser;
}

async function writeChangesToDb(
    userId: string,
    followedUsers: UserV2[],
    followedUserId: string,
    is_removed = false
) {
    const followedUserInfo = followedUsers.find((x) => x.id === followedUserId);
    const followedUser = followedUserInfo
        ? await upsertUser(followedUserInfo)
        : await prisma.tUser.findUnique({ where: { id: followedUserId } }); // If the info is undefined, this user is being removed

    const previousConnection = await prisma.tConnection.findFirst({
        where: { fromId: userId, toId: followedUser!.id },
        orderBy: { version: "desc" },
    });

    const version = previousConnection ? previousConnection.version + 1 : 0;
    const status = is_removed ? "DISCONNECTED" : "CONNECTED";
    const connection = await prisma.tConnection.create({
        data: {
            from: { connect: { id: userId } },
            to: { connect: { id: followedUser!.id } },
            version: version,
            status: status,
        },
    });

    return connection;
}

dotenv.config();
let SLEEP_TIME_MS = 60 * 1_000;
const SLEEP_MULTIPLIER_ON_ERROR = 1.1;
const REFRESH_THRESHOLD_IN_MS = 8 * 60 * 60 * 1000; // Every 12 hours
if (!process.env.TWITTER_API_BEARER)
    throw new Error(`wtfff${process.env.TWITTER_API_BEARER}`);

const twitterClient = new TwitterApi(process.env.TWITTER_API_BEARER);
const twtr = twitterClient.readOnly;

let SLEEP_UNTIL_TIMESTAMP_IN_MS = 0;

const loop = async () => {
    // Fetch stale user
    // Fetch following from database
    // Fetch following from twitter API
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
        orderBy: [{ fullFollowingScrapedAt: "asc" }],
    });
    log(
        `${new Date().toISOString()}\t${
            staleUser?.username
        }\t${staleUser?.fullFollowingScrapedAt.toISOString()}`
    );
    if (!staleUser) return;
    if (
        !(
            Date.now() - staleUser.fullFollowingScrapedAt.getTime() >
            REFRESH_THRESHOLD_IN_MS
        )
    ) {
        SLEEP_UNTIL_TIMESTAMP_IN_MS =
            staleUser.fullFollowingScrapedAt.getTime() +
            REFRESH_THRESHOLD_IN_MS;
        log(
            `${new Date().toISOString()}\tSLEEP UNTIL ${new Date(
                SLEEP_UNTIL_TIMESTAMP_IN_MS / 1000.0
            ).toISOString()}`
        );
        return;
    }

    const dbFollowing = await prisma.tConnection.findMany({
        where: {
            fromId: staleUser.id,
        },
        distinct: ["fromId", "toId"],
        orderBy: {
            version: "desc",
        },
        select: {
            toId: true,
            status: true,
        },
    });

    const dbFollowingIds = dbFollowing
        .filter((x) => x.status === "CONNECTED")
        .map((x) => x.toId);

    const twtrFollowingIds: string[] = [];
    const twtrFollowing: UserV2[] = [];
    const request = await twtr.v2.following(staleUser.id, {
        asPaginator: true,
        max_results: 1000,
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
    if (request.rateLimit.remaining < staleUser.lastFollowingCount / 1000) {
        log(
            `${new Date().toISOString()}\tCannot finish without exceeding ratelimit: ${
                request.rateLimit.remaining
            } < ${staleUser.lastFollowingCount / 1000} ${JSON.stringify(
                request.rateLimit
            )}`
        );
        SLEEP_UNTIL_TIMESTAMP_IN_MS = request.rateLimit.reset * 1000.0;
        return;
    }
    for await (const follower of request) {
        twtrFollowing.push(follower);
        twtrFollowingIds.push(follower.id);
    }

    const _old = new Set(dbFollowingIds);
    const _new = new Set(twtrFollowingIds);
    const added = new Set([..._new].filter((x) => !_old.has(x)));
    const removed = new Set([..._old].filter((x) => !_new.has(x)));
    for (const id of added) {
        await writeChangesToDb(staleUser.id, twtrFollowing, id, false);
    }
    for (const id of removed) {
        await writeChangesToDb(staleUser.id, twtrFollowing, id, true);
    }

    await prisma.tUser.update({
        where: { id: staleUser.id },
        data: { fullFollowingScrapedAt: new Date() },
    });
    log(
        `${new Date().toISOString()}\tFollowing: ${
            dbFollowingIds.length
        } Added: ${added.size}\tRemoved: ${removed.size}`
    );
};

export const scraperFollowing = async () => {
    await sleep(1000);
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
                    log(`${new Date().toISOString()}\tRatelimited: ${error}\t`);
                    SLEEP_UNTIL_TIMESTAMP_IN_MS =
                        error.rateLimit.reset * 1000.0;
                    SLEEP_TIME_MS *= SLEEP_MULTIPLIER_ON_ERROR;
                    log(
                        "\tSLEEP UNTIL: " +
                            new Date(SLEEP_UNTIL_TIMESTAMP_IN_MS).toISOString()
                    );
                } else if (
                    error instanceof ApiResponseError &&
                    (error.code == 500 ||
                        error.code == 502 ||
                        error.code == 503 ||
                        error.code == 504)
                ) {
                    log(`${new Date().toISOString()}\tAPI Error: ${error}\t`);
                    SLEEP_UNTIL_TIMESTAMP_IN_MS = Date.now() + 60 * 1000.0; // Wait one minute before retrying...
                } else if (
                    error instanceof Prisma.PrismaClientKnownRequestError &&
                    error.code === "P2002"
                ) {
                    // This is likely a race condition
                    log(
                        `${new Date().toISOString()}\tUnique constraint error: ${error}\t`
                    );
                    SLEEP_UNTIL_TIMESTAMP_IN_MS = Date.now() + 60 * 1000.0; // Wait one minute before retrying...
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

// scraperFollowing()
//     .then()
//     .catch((e) => {
//         log(e);
//         throw e;
//     });
