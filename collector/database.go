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
                video := &pb.Video{}
                err := rows.Scan(
                    &video.VideoId,
                    &video.ChannelId,
                    &video.CategoryId,
                    &video.Title,
                    &video.Description,
                    &video.PublishdAt,
                    &video.Duration,
                    &video.ActiveLiveChatId,
                    &video.ActualStartTime,
                    &video.ActualEndTime,
                    &video.ScheduledStartTime,
                    &video.ScheduledEndTime,
                    &video.StatusPrivacyStatus,
                    &video.StatusUploadStatus,
                    &video.StatusEmbeddable,
		    _,
                )
                if err != nil {
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
		categoryId
                title,
                description,
                publishdAt,
                duration,
                activeLiveChatId,
                actualStartTime,
                actualEndTime,
                scheduledStartTime,
                scheduledEndTime,
                statusPrivacyStatus,
                statusUploadStatus,
                statusEmbeddable,
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
	    video.PublishdAt,
	    video.Duration,
            video.ActiveLiveChatId,
            video.ActualStartTime,
            video.ActualEndTime,
            video.ScheduledStartTime,
            video.ScheduledEndTime,
	    video.StatusPrivacyStatus,
	    video.StatusUploadStatus,
            video.StatusEmbeddable,
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
		log.Printf("delete video (videoId = %v, rowsAffected = %v)", videoId, rowsAffected)
	}
        return nil
}

func (d *DatabaseOperator) GetActiveLiveChatMessagesByVideoIdAndToken(videoId string, token string) (string, []*pb.ActiveLiveChatMessage, error) {
	var nextToken string
	activeLiveChatMessages := make([]*pb.ActiveLiveChatMessage, 0)
        activeLiveChatMessageRows, err := d.db.Query(`SELECT * FROM activeLiveChatMessage WHERE videoId = ? AND token = ?`, videoId, token)
        if err != nil {
		return "", nil, fmt.Errorf("can not get activeLiveChatMessage by videoId and token: %w", err)
        }
        defer activeLiveChatMessageRows.Close()
        for activeLiveChatMessageRows.Next() {
                activeLiveChatMessage := &pb.ActiveLiveChatMessage{}
                err := activeLiveChatMessageRows.Scan(
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
		    _,
		    &nextToken,
		    _,
                )
                if err != nil {
			return "", nil, fmt.Errorf("can not scan activeLiveChatMessage by videoId and token: %w", err)
                }
		activeLiveChatMessages = append(activeLiveChatMessages, activeLiveChatMessage)
        }
        return nextToken, activeLiveChatMessages, nil
}

func (d *DatabaseOperator) UpdateActiveLiveChatMessages(token string, nextToken string, activeLiveChatMessages []*pb.ActiveLiveChatMessage) (error) {
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
			token,
			nextToken,
			lastUpdate
		    ) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		    )`,
		    activeLiveChatMessage.messageId,
		    activeLiveChatMessage.ChannelId,
		    activeLiveChatMessage.VideoId,
		    activeLiveChatMessage.apiEtag,
		    activeLiveChatMessage.authorChannelId,
		    activeLiveChatMessage.authorChannelUrl,
		    activeLiveChatMessage.authorDisplayName,
		    activeLiveChatMessage.authorIsChatModerator,
		    activeLiveChatMessage.authorIsChatOwner,
		    activeLiveChatMessage.authorIsChatSponsor,
		    activeLiveChatMessage.authorIsVerified,
		    activeLiveChatMessage.liveChatId,
		    activeLiveChatMessage.displayMessage,
		    activeLiveChatMessage.publishedAt,
		    activeLiveChatMessage.isSuperChat,
		    activeLiveChatMessage.amountDisplayString,
		    activeLiveChatMessage.currency,
		    token,
		    nextToken,
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
			log.Printf("update activeLiveChatMessage (messageId = %v, insert id = %v)", activeLiveChatMessage.messageId, id)
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
		log.Printf("delete activeLiveChatMessages (videoId = %v, rowsAffected = %v)", videoId, rowsAffected)
	}

        return nil
}

func (d *DatabaseOperator) GetArchiveLiveChatMessagesByVideoIdAndToken(videoId string, token string) (string, []*pb.ArchiveLiveChatMessage, error) {
	var nextToken string
	archiveLiveChatMessages := make([]*pb.ArchiveLiveChatMessage, 0)
        archiveLiveChatMessageRows, err := d.db.Query(`SELECT * FROM archiveLiveChatMessage WHERE videoId = ? AND token = ?`, videoId, token)
        if err != nil {
		return "", nil, fmt.Errorf("can not get archiveLiveChatMessage by videoId and token: %w", err)
        }
        defer archiveLiveChatMessageRows.Close()
        for archiveLiveChatMessageRows.Next() {
                archiveLiveChatMessage := &ArchiveLiveChatMessage{}
                err := archiveLiveChatMessageRows.Scan(
		    &archiveLiveChatMessage.UniqueId,
		    &archiveLiveChatMessage.ChannelId,
		    &archiveLiveChatMessage.VideoId,
		    &archiveLiveChatMessage.ClientId,
		    &archiveLiveChatMessage.MessageId,
		    &archiveLiveChatMessage.TimestampAt,
		    &archiveLiveChatMessage.TimestampText,
		    &archiveLiveChatMessage.AuthorName,
		    &archiveLiveChatMessage.MessageText,
		    &archiveLiveChatMessage.PurchaseAmountText,
		    &archiveLiveChatMessage.VideoOffsetTimeMsec,
		    _,
		    &nextToken,
		    _,
                )
                if err != nil {
			return "", nil, fmt.Errorf("can not scan archiveLiveChatMessage by videoId and token: %w", err)
                }
		archiveLiveChatMessages = append(archiveLiveChatMessages, archiveLiveChatMessage)
        }
        return nextToken, archiveLiveChatMessages, nil
}

func (d *DatabaseOperator) CountArchiveLiveChatMessagesByVideoId(videoId string) (int, error) {
	var nextToken string
	archiveLiveChatMessages := make([]*pb.ArchiveLiveChatMessage, 0)
        archiveLiveChatMessageRows, err := d.db.Query(`SELECT count(*) FROM archiveLiveChatMessage WHERE videoId = ?`, videoId)
        if err != nil {
		return "", nil, fmt.Errorf("can not get archiveLiveChatMessage by videoId: %w", err)
        }
        defer archiveLiveChatMessageRows.Close()
        for archiveLiveChatMessageRows.Next() {
		var count int
                err := archiveLiveChatMessageRows.Scan(
		    &count,
                )
                if err != nil {
			return 0, fmt.Errorf("can not scan archiveLiveChatMessage by videoId: %w", err)
                }
		return count, nil
        }
        return 0, nil
}

func (d *DatabaseOperator) UpdateArchiveLiveChatMessages(token string, nextToken string, archiveLiveChatMessages []*pb.ArchiveLiveChatMessage) (error) {
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
			timestampUsec,
			clientId,
			authorName,
			messageText,
			purchaseAmountText,
			videoOffsetTimeMsec,
			token,
			nextToken,
			lastUpdate
		    ) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?
		    )`,
		    archiveLiveChatMessage.MessageId,
		    archiveLiveChatMessage.ChannelId,
		    archiveLiveChatMessage.VideoId,
		    archiveLiveChatMessage.TimestampUsec,
		    archiveLiveChatMessage.ClientId,
		    archiveLiveChatMessage.AuthorName,
		    archiveLiveChatMessage.MessageText,
		    archiveLiveChatMessage.PurchaseAmountText,
		    archiveLiveChatMessage.VideoOffsetTimeMsec,
		    token,
		    nextToken,
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
			log.Printf("update archiveLiveChatMessage (messageId = %v, insert id = %v)", archiveLiveChatMessage.messageId, id)
		}
	}
	tx.Commit()
	return nil
}

func (d *DatabaseOperator) DeleteArchiveLiveChatMessagesByLastUpdate(videoId string) (error) {
	res, err := d.db.Exec(`DELETE FROM archiveLiveChatMessage WHERE lastUpdate = ?`, lastUpdate)
        if err != nil {
		return fmt.Errorf("can not delete archiveLiveChatMessages: %w", err)
        }
        rowsAffected, err := res.RowsAffected()
        if err != nil {
		return fmt.Errorf("can not get rowsAffected of archiveLiveChatMessage: %w", err)
        }
	if d.verbose {
		log.Printf("delete archiveLiveChatMessages (videoId = %v, rowsAffected = %v)", videoId, rowsAffected)
	}
        return nil
}

func (d *DatabaseOperator) createTables() (error) {
        videoTableCreateQuery := `
            CREATE TABLE IF NOT EXISTS video (
                videoId            TEXT PRIMARY KEY,
                channelId          TEXT NOT NULL,
		categoryId         TEXT NOT NULL
                title              TEXT NOT NULL,
                description        TEXT NOT NULL,
		publishdAt         TEXT NOT NULL,
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
	_, err = d.db.Exec(videoTableCreateQuery);
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
		pageToken             TEXT NOT NULL,
		nextPageTolken        TEXT NOT NULL,
		authorChannelId       TEXT NOT NULL,
		authorChannelUrl      TEXT NOT NULL,
		authorDisplayName     TEXT NOT NULL,
		authorIsChatModerator TEXT NOT NULL,
		authorIsChatOwner     TEXT NOT NULL,
		authorIsChatSponsor   TEXT NOT NULL,
		authorIsVerified      TEXT NOT NULL,
		liveChatId            TEXT NOT NULL,
		displayMessage        TEXT NOT NULL,
		messagePublishedAt    TEXT NOT NULL,
		isSuperChat           TEXT NOT NULL,
		amountDisplayString   TEXT NOT NULL,
		currency              TEXT NOT NULL,
		Token                 TEXT NOT NULL,
		nextToken             TEXT NOT NULL,
		lastUpdate            INTEGER NOT NULL
	)`
	_, err = d.db.Exec(avtiveLiveChatMessageTableCreateQuery);
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
	_, err = d.db.Exec(activeLiveChatMessageLastUPdateIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create lastUpdate index of activeLiveChatMessage: %w", err)
	}

        archiveLiveChatMessageTableCreateQuery := `
            CREATE TABLE IF NOT EXISTS archiveLiveChatMessage (
		messageId           TEXT PRIMARY KEY,
		channelId           TEXT NOT NULL,
		videoId             TEXT NOT NULL,
		clientId            TEXT NOT NULL,
		timestampAt         TEXT NOT NULL,
		timestampText       TEXT NOT NULL,
		authorName          TEXT NOT NULL,
		messageText         TEXT NOT NULL,
		purchaseAmountText  TEXT NOT NULL,
		videoOffsetTimeMsec TEXT NOT NULL,
		Token               TEXT NOT NULL,
		nextToken           TEXT NOT NULL,
		lastUpdate          INTEGER NOT NULL
	)`
	_, err = d.db.Exec(archiveLiveChatMessageTableCreateQuery);
	if  err != nil {
		return fmt.Errorf("can not create archiveLiveChatMessage table: %w", err)
	}
        archiveLiveChatMessageVideoIdIndexQuery := `CREATE INDEX IF NOT EXISTS archiveLiveChatMessageVideoIdIndex ON liveChatMessage(videoId)`
	_, err = d.db.Exec(archiveLiveChatMessageVideoIdIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create vodeoId index of archiveLiveChatMessage: %w", err)
	}
        archiveLiveChatMessageChannelIdIndexQuery := `CREATE INDEX IF NOT EXISTS archiveLiveChatMessageChannelIdIndex ON liveChatMessage(channelId)`
	_, err = d.db.Exec(archiveLiveChatMessageChannelIdIndexQuery);
	if  err != nil {
		return fmt.Errorf("can not create channelId index archiveLiveChatMessage: %w", err)
	}
        archiveLiveChatMessageLastUpdateIndexQuery := `CREATE INDEX IF NOT EXISTS archiveLiveChatMessageLastUPdateIndex ON liveChatMessage(lastUpdate)`
	_, err = d.db.Exec(archiveLiveChatMessageLastUPdateIndexQuery);
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

func NewDatabaseOperator(verbose bool, databasePath string) (*DatabaseOperator, error) {
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
		verbose: verbose,
                databasePath: databasePath,
                db: nil,
        }, nil
}
