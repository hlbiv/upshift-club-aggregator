import { Router, type IRouter } from "express";
import healthRouter from "./health";
import clubsRouter from "./clubs";
import leaguesRouter from "./leagues";
import searchRouter from "./search";
import eventsRouter from "./events";
import coachesRouter from "./coaches";
import analyticsRouter from "./analytics";
import collegesRouter from "./colleges";

const router: IRouter = Router();

router.use(healthRouter);
router.use(clubsRouter);
router.use(leaguesRouter);
router.use(searchRouter);
router.use(eventsRouter);
router.use(coachesRouter);
router.use(analyticsRouter);
router.use(collegesRouter);

export default router;
