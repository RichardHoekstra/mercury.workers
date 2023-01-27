import { scraperFollowing } from "./scrape_following";
import { scraperLikes } from "./scrape_likes";
import { scraperProfiles } from "./scrape_profiles";

export const scrapers = async () => {
    await Promise.all([scraperProfiles(), scraperLikes(), scraperFollowing()]);
};

scrapers()
    .then()
    .catch((e) => {
        console.log(e);
        throw e;
    });
