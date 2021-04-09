package collector

import (
        "fmt"
        "os"
        "log"
        "time"
        "path/filepath"
        "database/sql"
        _ "github.com/mattn/go-sqlite3"
	pb "github.com/potix/ylcc/protocol"
)

type DatabaseOperator struct {
	verbose      bool
        databasePath string
        db           *sql.DB
}

func (d *DatabaseOperator) GetVideoByVideoId(videoId string) (*pb.Video, bool, error) {
        rows, err := d.db.Query(`SELECT * FROM video WHERE videoId = ?`, videoId)
        if err != nil {
		return nil, false, fmt.Errorf("can not get video by videoId: %w", err)
        }
        defer rows.Close()
        for rows.Next() {
		var lastUpdate int
                video := &pb.Video{}
		if err := rows.Scan(
                    &video.VideoId,
                    &video.ChannelId,
                    &video.CategoryId,
                    &video.Title,
                    &video.Description,
                    &video.PublishedAt,
                    &video.Duration,
                    &video.ActiveLiveChatId,
                    &video.ActualStartTime,
                    &video.ActualEndTime,
                    &video.ScheduledStartTime,
                    &video.ScheduledEndTime,
                    &video.PrivacyStatus,
                    &video.UploadStatus,
                    &video.Embeddable,
		    &lastUpdate,
                ); err != nil {
			return nil, false, fmt.Errorf("can not scan video by videoId: %w", err)
                }
		return video, true, nil
        }
        return nil, false, nil
}

func (d *DatabaseOperator) UpdateVideo(video *pb.Video) (error) {
	res, err := d.db.Exec(
            `INSERT OR REPLACE INTO video (
                videoId,
                channelId,
		categoryId,
                title,
                description,
                publishedAt,
                duration,
                activeLiveChatId,
                actualStartTime,
                actualEndTime,
                scheduledStartTime,
                scheduledEndTime,
                privacyStatus,
                uploadStatus,
                embeddable,
		lastUpdate
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?
            )`,
	    video.VideoId,
	    video.ChannelId,
	    video.CategoryId,
	    video.Title,
	    video.Description,
	    video.PublishedAt,
	    video.Duration,
            video.ActiveLiveChatId,
            video.ActualStartTime,
            video.ActualEndTime,
            video.ScheduledStartTime,
            video.ScheduledEndTime,
	    video.PrivacyStatus,
	    video.UploadStatus,
            video.Embeddable,
	    time.Now().Unix(),
        )
        if err != nil {
		return fmt.Errorf("can not insert video: %w", err)
        }
        id, err := res.LastInsertId()
        if err != nil {
		return fmt.Errorf("can not get insert id of video: %w", err)
        }
	if d.verbose {
		log.Printf("update video (videoId = %v, insert id = %v)", video.VideoId, id)
	}
        return nil
}

func (d *DatabaseOperator) DeleteVideoByLastUpdate(lastUpdate int) (error) {
	res, err := d.db.Exec(`DELETE FROM video WHERE lastUpdate < ?`, lastUpdate)
        if err != nil {
		return fmt.Errorf("can not delete video: %w", err)
        }
        rowsAffected, err := res.RowsAffected()
        if err != nil {
		return fmt.Errorf("can not get rowsAffected of video: %w", err)
        }
	if d.verbose {
		log.Printf("delete video (rowsAffected = %v)", rowsAffected)
	}
        return nil
}

func (d *DatabaseOperator) GetActiveLiveChatMessagesByVideoIdAndToken(videoId string, offset int64, count int64) ([]*pb.ActiveLiveChatMessage, error) {
	activeLiveChatMessages := make([]*pb.ActiveLiveChatMessage, 0)
        activeLiveChatMessageRows, err := d.db.Query(`SELECT * FROM activeLiveChatMessage WHERE videoId = ? LIMIT ? OFFSET ?`, videoId, count, offset)
        if err != nil {
		return nil, fmt.Errorf("can not get activeLiveChatMessage by videoId and token: %w", err)
        }
        defer activeLiveChatMessageRows.Close()
        for activeLiveChatMessageRows.Next() {
		var lastUpdate int
                activeLiveChatMessage := &pb.ActiveLiveChatMessage{}
                if err := activeLiveChatMessageRows.Scan(
		    &activeLiveChatMessage.MessageId,
		    &activeLiveChatMessage.ChannelId,
		    &activeLiveChatMessage.VideoId,
		    &activeLiveChatMessage.ApiEtag,
		    &activeLiveChatMessage.AuthorChannelId,
		    &activeLiveChatMessage.AuthorChannelUrl,
		    &activeLiveChatMessage.AuthorDisplayName,
		    &activeLiveChatMessage.AuthorIsChatModerator,
		    &activeLiveChatMessage.AuthorIsChatOwner,
		    &activeLiveChatMessage.AuthorIsChatSponsor,
		    &activeLiveChatMessage.AuthorIsVerified,
		    &activeLiveChatMessage.LiveChatId,
		    &activeLiveChatMessage.DisplayMessage,
		    &activeLiveChatMessage.PublishedAt,
		    &activeLiveChatMessage.IsSuperChat,
		    &activeLiveChatMessage.AmountDisplayString,
		    &activeLiveChatMessage.Currency,
		    &activeLiveChatMessage.PageToken,
		    &lastUpdate,
                ); err != nil {
			return nil, fmt.Errorf("can not scan activeLiveChatMessage by videoId and token: %w", err)
                }
		activeLiveChatMessages = append(activeLiveChatMessages, activeLiveChatMessage)
        }
        return activeLiveChatMessages, nil
}

func (d *DatabaseOperator) UpdateActiveLiveChatMessages(activeLiveChatMessages []*pb.ActiveLiveChatMessage) (error) {
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("can not start transaction in UpdateActiveLiveChatMessages: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()
	nowUnix := time.Now().Unix()
	for _, activeLiveChatMessage := range activeLiveChatMessages {
		res, err := tx.Exec(
		    `INSERT OR REPLACE INTO activeLiveChatMessage (
			messageId,
			channelId,
			videoId,
			apiEtag,
			authorChannelId,
			authorChannelUrl,
			authorDisplayName,
			authorIsChatModerator,
			authorIsChatOwner,
			authorIsChatSponsor,
			authorIsVerified,
			liveChatId,
			displayMessage,
			publishedAt,
			isSuperChat,
			amountDisplayString,
			currency,
			pageToken,
			lastUpdate
		    ) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?
		    )`,
		    activeLiveChatMessage.MessageId,
		    activeLiveChatMessage.ChannelId,
		    activeLiveChatMessage.VideoId,
		    activeLiveChatMessage.ApiEtag,
		    activeLiveChatMessage.AuthorChannelId,
		    activeLiveChatMessage.AuthorChannelUrl,
		    activeLiveChatMessage.AuthorDisplayName,
		    activeLiveChatMessage.AuthorIsChatModerator,
		    activeLiveChatMessage.AuthorIsChatOwner,
		    activeLiveChatMessage.AuthorIsChatSponsor,
		    activeLiveChatMessage.AuthorIsVerified,
		    activeLiveChatMessage.LiveChatId,
		    activeLiveChatMessage.DisplayMessage,
		    activeLiveChatMessage.PublishedAt,
		    activeLiveChatMessage.IsSuperChat,
		    activeLiveChatMessage.AmountDisplayString,
		    activeLiveChatMessage.Currency,
		    activeLiveChatMessage.PageToken,
		    nowUnix,
		)
		if err != nil {
		        tx.Rollback()
			return fmt.Errorf("can not insert activeLiveChatMessage: %w", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
		        tx.Rollback()
			return fmt.Errorf("can not get insert id of activeLiveChatMessage: %w", err)
		}
		if d.verbose {
			log.Printf("update activeLiveChatMessage (messageId = %v, insert id = %v)", activeLiveChatMessage.MessageId, id)
		}
	}
	tx.Commit()
	return nil
}

func (d *DatabaseOperator) DeleteActiveLiveChatMessagesByLastUpdate(lastUpdate int) (error) {
	res, err := d.db.Exec(`DELETE FROM activeLiveChatMessage WHERE lastUpdate < ?`, lastUpdate)
        if err != nil {
		return fmt.Errorf("can not delete activeLiveChatMessages: %w", err)
        }
        rowsAffected, err := res.RowsAffected()
        if err != nil {
		return fmt.Errorf("can not get rowsAffected of activeLiveChatMessage: %w", err)
        }
	if d.verbose {
		log.Printf("delete activeLiveChatMessages (lastUpdate = %v, rowsAffected = %v)",lastUpdate, rowsAffected)
	}

        return nil
}

func (d *DatabaseOperator) GetArchiveLiveChatMessagesByVideoIdAndToken(videoId string, offset int64, count int64) ([]*pb.ArchiveLiveChatMessage, error) {
	archiveLiveChatMessages := make([]*pb.ArchiveLiveChatMessage, 0)
        archiveLiveChatMessageRows, err := d.db.Query(`SELECT * FROM archiveLiveChatMessage WHERE videoId = ? LIMIT ? OFFSET ?`, videoId, count, offset)
        if err != nil {
		return nil, fmt.Errorf("can not get archiveLiveChatMessage by videoId and token: %w", err)
        }
        defer archiveLiveChatMessageRows.Close()
        for archiveLiveChatMessageRows.Next() {
		var lastUpdate int
                archiveLiveChatMessage := &pb.ArchiveLiveChatMessage{}
                if err := archiveLiveChatMessageRows.Scan(
		    &archiveLiveChatMessage.MessageId,
		    &archiveLiveChatMessage.ChannelId,
		    &archiveLiveChatMessage.VideoId,
		    &archiveLiveChatMessage.ClientId,
		    &archiveLiveChatMessage.AuthorName,
		    &archiveLiveChatMessage.AuthorExternalChannelId,
		    &archiveLiveChatMessage.MessageText,
		    &archiveLiveChatMessage.PurchaseAmountText,
		    &archiveLiveChatMessage.IsPaid,
		    &archiveLiveChatMessage.TimestampUsec,
		    &archiveLiveChatMessage.TimestampText,
		    &archiveLiveChatMessage.VideoOffsetTimeMsec,
		    &archiveLiveChatMessage.Continuation,
		    &lastUpdate,
                ); err != nil {
			return nil, fmt.Errorf("can not scan archiveLiveChatMessage by videoId and token: %w", err)
                }
		archiveLiveChatMessages = append(archiveLiveChatMessages, archiveLiveChatMessage)
        }
        return archiveLiveChatMessages, nil
}

func (d *DatabaseOperator) CountArchiveLiveChatMessagesByVideoId(videoId string) (int, error) {
        archiveLiveChatMessageRows, err := d.db.Query(`SELECT count(*) FROM archiveLiveChatMessage WHERE videoId = ?`, videoId)
        if err != nil {
		return -1, fmt.Errorf("can not get archiveLiveChatMessage by videoId: %w", err)
        }
        defer archiveLiveChatMessageRows.Close()
        for archiveLiveChatMessageRows.Next() {
		var count int
                if err := archiveLiveChatMessageRows.Scan(&count); err != nil {
			return 0, fmt.Errorf("can not scan archiveLiveChatMessage by videoId: %w", err)
                }
		return count, nil
        }
        return 0, nil
}

func (d *DatabaseOperator) UpdateArchiveLiveChatMessages(archiveLiveChatMessages []*pb.ArchiveLiveChatMessage) (error) {
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("can not start transaction in UpdateArchiveLiveChatMessages: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()
	nowUnix := time.Now().Unix()
	for _, archiveLiveChatMessage := range archiveLiveChatMessages {
		res, err := tx.Exec(
		    `INSERT OR REPLACE INTO archiveLiveChatMessage (
			messageId,
			channelId,
			videoId,
			clientId,
			authorName,
			authorExternalChannelId,
			messageText,
			purchaseAmountText,
			isPaid,
			timestampUsec,
			timestampText,
			videoOffsetTimeMsec,
			continuation,
			lastUpdate
		    ) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?
		    )`,
		    archiveLiveChatMessage.MessageId,
		    archiveLiveChatMessage.ChannelId,
		    archiveLiveChatMessage.VideoId,
		    archiveLiveChatMessage.ClientId,
		    archiveLiveChatMessage.AuthorName,
		    archiveLiveChatMessage.AuthorExternalChannelId,
		    archiveLiveChatMessage.MessageText,
		    archiveLiveChatMessage.PurchaseAmountText,
		    archiveLiveChatMessage.IsPaid,
		    archiveLiveChatMessage.TimestampUsec,
		    archiveLiveChatMessage.TimestampText,
		    archiveLiveChatMessage.VideoOffsetTimeMsec,
		    archiveLiveChatMessage.Continuation,
		    nowUnix,
		)
		if err != nil {
		        tx.Rollback()
			return fmt.Errorf("can not insert archiveLiveChatMessage: %w", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
		        tx.Rollback()
			return fmt.Errorf("can not get insert id of archiveLiveChatMessage: %w", err)
		}
		if d.verbose {
			log.Printf("update archiveLiveChatMessage (messageId = %v, insert id = %v)", archiveLiveChatMessage.MessageId, id)
		}
	}
	tx.Commit()
	return nil
}

func (d *DatabaseOperator) DeleteArchiveLiveChatMessagesByLastUpdate(lastUpdate int) (error) {
	res, err := d.db.Exec(`DELETE FROM archiveLiveChatMessage WHERE lastUpdate = ?`, lastUpdate)
        if err != nil {
		return fmt.Errorf("can not delete archiveLiveChatMessages: %w", err)
        }
        rowsAffected, err := res.RowsAffected()
        if err != nil {
		return fmt.Errorf("can not get rowsAffected of archiveLiveChatMessage: %w", err)
        }
	if d.verbose {
		log.Printf("delete archiveLiveChatMessages (lastUpdate = %v, rowsAffected = %v)", lastUpdate, rowsAffected)
	}
        return nil
}

func (d *DatabaseOperator) createTables() (error) {
        videoTableCreateQuery := `
            CREATE TABLE IF NOT EXISTS video (
                videoId            TEXT PRIMARY KEY,
                channelId          TEXT NOT NULL,
		categoryId         TEXT NOT NULL,
                title              TEXT NOT NULL,
                description        TEXT NOT NULL,
		publishedAt        TEXT NOT NULL,
		duration           TEXT NOT NULL,
		activeLiveChatId   TEXT NOT NULL,
		actualStartTime    TEXT NOT NULL,
		actualEndtime      TEXT NOT NULL,
		scheduledStartTime TEXT NOT NULL,
		scheduledEndTime   TEXT NOT NULL,
		privacyStatus      TEXT NOT NULL,
		uploadStatus       TEXT NOT NULL,
		embeddable         TEXT NOT NULL,
		lastUpdate         INTEGER NOT NULL
	)`
	_, err := d.db.Exec(videoTableCreateQuery);
	if  err != nil {
		return fmt.Errorf("can not create video table: %w", err)
	}
        videoLastUpdateIndexQuery := `CREATE INDEX IF NOT EXISTS videoLastUpdateIndex ON video(lastUpdate)`
	_, err = d.db.Exec(videoLastUpdateIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create lastUpdate index of video: %w", err)
	}

        activeLiveChatMessageTableCreateQuery := `
            CREATE TABLE IF NOT EXISTS activeLiveChatMessage (
                messageId             TEXT PRIMARY KEY,
		channelId             TEXT NOT NULL,
		videoId               TEXT NOT NULL,
		apiEtag               TEXT NOT NULL,
		authorChannelId       TEXT NOT NULL,
		authorChannelUrl      TEXT NOT NULL,
		authorDisplayName     TEXT NOT NULL,
		authorIsChatModerator TEXT NOT NULL,
		authorIsChatOwner     TEXT NOT NULL,
		authorIsChatSponsor   TEXT NOT NULL,
		authorIsVerified      TEXT NOT NULL,
		liveChatId            TEXT NOT NULL,
		displayMessage        TEXT NOT NULL,
		publishedAt           TEXT NOT NULL,
		isSuperChat           INTEGER NOT NULL,
		amountDisplayString   TEXT NOT NULL,
		currency              TEXT NOT NULL,
		pageToken             TEXT NOT NULL,
		lastUpdate            INTEGER NOT NULL
	)`
	_, err = d.db.Exec(activeLiveChatMessageTableCreateQuery);
	if  err != nil {
		return fmt.Errorf("can not create activeLiveChatMessage table: %w", err)
	}
        activeLiveChatMessageVideoIdIndexQuery := `CREATE INDEX IF NOT EXISTS activeLiveChatMessageVideoIdIndex ON activeLiveChatMessage(videoId)`
	_, err = d.db.Exec(activeLiveChatMessageVideoIdIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create videoId index of avtiveLiveChatMessage: %w", err)
	}
        activeLiveChatMessageChannelIdIndexQuery := `CREATE INDEX IF NOT EXISTS activeLiveChatMessageChannelIdIndex ON activeLiveChatMessage(channelId)`
	_, err = d.db.Exec(activeLiveChatMessageChannelIdIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create channelId index of activeLiveChatMessage: %w", err)
	}
        activeLiveChatMessageLastUpdateIndexQuery := `CREATE INDEX IF NOT EXISTS activeLiveChatMessageLastUPdateIndex ON activeLiveChatMessage(lastUpdate)`
	_, err = d.db.Exec(activeLiveChatMessageLastUpdateIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create lastUpdate index of activeLiveChatMessage: %w", err)
	}

        archiveLiveChatMessageTableCreateQuery := `
            CREATE TABLE IF NOT EXISTS archiveLiveChatMessage (
		messageId               TEXT PRIMARY KEY,
		channelId               TEXT NOT NULL,
		videoId                 TEXT NOT NULL,
		clientId                TEXT NOT NULL,
		authorName              TEXT NOT NULL,
		authorExternalChannelId TEXT NOT NULL,
		messageText             TEXT NOT NULL,
		purchaseAmountText      TEXT NOT NULL,
		isPaid                  INTEGER NOT NULL,
		timestampUsec           TEXT NOT NULL,
		timestampText           TEXT NOT NULL,
		videoOffsetTimeMsec     TEXT NOT NULL,
		continuation            TEXT NOT NULL,
		lastUpdate              INTEGER NOT NULL
	)`
	_, err = d.db.Exec(archiveLiveChatMessageTableCreateQuery);
	if  err != nil {
		return fmt.Errorf("can not create archiveLiveChatMessage table: %w", err)
	}
        archiveLiveChatMessageVideoIdIndexQuery := `CREATE INDEX IF NOT EXISTS archiveLiveChatMessageVideoIdIndex ON archiveLiveChatMessage(videoId)`
	_, err = d.db.Exec(archiveLiveChatMessageVideoIdIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create vodeoId index of archiveLiveChatMessage: %w", err)
	}
        archiveLiveChatMessageChannelIdIndexQuery := `CREATE INDEX IF NOT EXISTS archiveLiveChatMessageChannelIdIndex ON archiveLiveChatMessage(channelId)`
	_, err = d.db.Exec(archiveLiveChatMessageChannelIdIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create channelId index archiveLiveChatMessage: %w", err)
	}
        archiveLiveChatMessageLastUpdateIndexQuery := `CREATE INDEX IF NOT EXISTS archiveLiveChatMessageLastUPdateIndex ON archiveLiveChatMessage(lastUpdate)`
	_, err = d.db.Exec(archiveLiveChatMessageLastUpdateIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create lastUPdate of archiveLiveChatMessage: %w", err)
	}

	return nil
}

func (d *DatabaseOperator) Open() (error) {
        db, err := sql.Open("sqlite3", d.databasePath)
        if err != nil {
		return fmt.Errorf("can not open database: %w", err)
        }
        d.db = db
        err = d.createTables()
        if err != nil {
		return fmt.Errorf("can not create table of database: %w", err)
        }
        return nil
}

func (d *DatabaseOperator) Close()  {
        d.db.Close()
}

func NewDatabaseOperator(databasePath string, opts ...Option) (*DatabaseOperator, error) {
	baseOpts := defaultOptions()
        for _, opt := range opts {
                opt(baseOpts)
        }
        if databasePath == "" {
                return nil, fmt.Errorf("no database path")
        }
        dirname := filepath.Dir(databasePath)
        _, err := os.Stat(dirname)
        if err != nil {
                err := os.MkdirAll(dirname, 0755)
                if err != nil {
                        return nil, fmt.Errorf("can not create directory (%v)", dirname)
                }
        }
        return &DatabaseOperator{
		verbose: baseOpts.verbose,
                databasePath: databasePath,
                db: nil,
        }, nil
}
