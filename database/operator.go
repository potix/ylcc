package database

import (
        "os"
        "log"
        "path/filepath"
        "github.com/pkg/errors"
        "database/sql"
        _ "github.com/mattn/go-sqlite3"
)

type DatabaseOperator struct {
        databasePath string
        db           *sql.DB
	verbose      bool
}

type Video struct {
	VideoId            string
	ChannelId          string
	CategoryId         string
	Title              string
	Description        string
	PublishdAt         string
	Duration           string
        ActiveLiveChatId   string
        ActualStartTime    string
        ActualEndTime      string
        ScheduledStartTime string
        ScheduledEndTime   string
        PrivacyStatus      string
        UploadStatus       string
        Embeddable         bool
}

type ActiveLiveChatMessage struct {
	UniqueId            string
	ChannelId           string
	VideoId             string
	ClientId            string
	MessageId           string
	TimestampAt         string
	TimestampText       string
	AuthorName          string
	AuthorPhotoUrl      string
	MessageText         string
	PurchaseAmountText  string
	VideoOffsetTimeMsec string
}

type LiveChatMessage struct {
	UniqueId            string
	ChannelId           string
	VideoId             string
	ClientId            string
	MessageId           string
	TimestampAt         string
	TimestampText       string
	AuthorName          string
	AuthorPhotoUrl      string
	MessageText         string
	PurchaseAmountText  string
	VideoOffsetTimeMsec string
}




func (d *DatabaseOperator) DeleteLiveChatMessagesByVideoId(videoId string) (error) {
	res, err := d.db.Exec(`DELETE FROM liveChatMessage WHERE videoId = ?`, videoId)
        if err != nil {
                return errors.Wrap(err, "can not delete liveChatMessages")
        }
        // 削除処理の結果から削除されたレコード数を取得
        rowsAffected, err := res.RowsAffected()
        if err != nil {
                return errors.Wrap(err, "can not get rowsAffected of liveChatMessage")
        }
	if d.verbose {
		log.Printf("delete liveChatMessages (videoId = %v, rowsAffected = %v)", videoId, rowsAffected)
	}

        return nil
}

func (d *DatabaseOperator) UpdateLiveChatMessages(liveChatMessages []*LiveChatMessage) (error) {
	tx, err := d.db.Begin()
	if err != nil {
		return errors.Wrap(err, "can not start transaction in update live chat")
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()
	for _, liveChatMessage := range liveChatMessages {
		res, err := tx.Exec(
		    `INSERT OR REPLACE INTO liveChatMessage (
			uniqueId,
			channelId,
			videoId,
			clientId,
			messageId,
			timestampAt,
			timestampText,
			authorName,
			authorPhotoUrl,
			messageText,
			purchaseAmountText,
			videoOffsetTimeMsec
		    ) VALUES (
			?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?
		    )`,
		    liveChatMessage.UniqueId,
		    liveChatMessage.ChannelId,
		    liveChatMessage.VideoId,
		    liveChatMessage.ClientId,
		    liveChatMessage.MessageId,
		    liveChatMessage.TimestampAt,
		    liveChatMessage.TimestampText,
		    liveChatMessage.AuthorName,
		    liveChatMessage.AuthorPhotoUrl,
		    liveChatMessage.MessageText,
		    liveChatMessage.PurchaseAmountText,
		    liveChatMessage.VideoOffsetTimeMsec,
		)
		if err != nil {
		        tx.Rollback()
			return errors.Wrap(err, "can not insert liveChatMessage")
		}
		// 挿入処理の結果からIDを取得
		id, err := res.LastInsertId()
		if err != nil {
		        tx.Rollback()
			return errors.Wrap(err, "can not get insert id of liveChatMessage")
		}
		if d.verbose {
			log.Printf("update live chat comment (uniqueId = %v, insert id = %v)", liveChatMessage.UniqueId, id)
		}
	}
	tx.Commit()
	return nil
}

func (d *DatabaseOperator) GetLiveChatMessagesByChannelId(channelId string) ([]*LiveChatMessage, error) {
	liveChatMessages := make([]*LiveChatMessage, 0)
        liveChatMessageRows, err := d.db.Query(`SELECT * FROM liveChatMessage Where channelId = ?`, channelId)
        if err != nil {
                return nil, errors.Wrap(err, "can not get liveChatMessage by channelId from database")
        }
        defer liveChatMessageRows.Close()
        for liveChatMessageRows.Next() {
                liveChatMessage := &LiveChatMessage{}
                // カーソルから値を取得
                err := liveChatMessageRows.Scan(
		    &liveChatMessage.UniqueId,
		    &liveChatMessage.ChannelId,
		    &liveChatMessage.VideoId,
		    &liveChatMessage.ClientId,
		    &liveChatMessage.MessageId,
		    &liveChatMessage.TimestampAt,
		    &liveChatMessage.TimestampText,
		    &liveChatMessage.AuthorName,
		    &liveChatMessage.AuthorPhotoUrl,
		    &liveChatMessage.MessageText,
		    &liveChatMessage.PurchaseAmountText,
		    &liveChatMessage.VideoOffsetTimeMsec,
                )
                if err != nil {
                        return nil, errors.Wrap(err, "can not scan liveChatMessage by channelId from database")
                }
		liveChatMessages = append(liveChatMessages, liveChatMessage)
        }
        return liveChatMessages, nil
}

func (d *DatabaseOperator) GetLiveChatMessagesByVideoId(videoId string) ([]*LiveChatMessage, error) {
	liveChatMessages := make([]*LiveChatMessage, 0)
        liveChatMessageRows, err := d.db.Query(`SELECT * FROM liveChatMessage Where videoId = ?`, videoId)
        if err != nil {
                return nil, errors.Wrap(err, "can not get liveChatMessage by videoId from database")
        }
        defer liveChatMessageRows.Close()
        for liveChatMessageRows.Next() {
                liveChatMessage := &LiveChatMessage{}
                // カーソルから値を取得
                err := liveChatMessageRows.Scan(
		    &liveChatMessage.UniqueId,
		    &liveChatMessage.ChannelId,
		    &liveChatMessage.VideoId,
		    &liveChatMessage.ClientId,
		    &liveChatMessage.MessageId,
		    &liveChatMessage.TimestampAt,
		    &liveChatMessage.TimestampText,
		    &liveChatMessage.AuthorName,
		    &liveChatMessage.AuthorPhotoUrl,
		    &liveChatMessage.MessageText,
		    &liveChatMessage.PurchaseAmountText,
		    &liveChatMessage.VideoOffsetTimeMsec,
                )
                if err != nil {
                        return nil, errors.Wrap(err, "can not scan liveChatMessage by videoId from database")
                }
		liveChatMessages = append(liveChatMessages, liveChatMessage)
        }
        return liveChatMessages, nil
}

func (d *DatabaseOperator) DeleteReplyMessagesByVideoId(videoId string) (error) {
	res, err := d.db.Exec(`DELETE FROM replyMessage WHERE videoId = ?`, videoId)
        if err != nil {
                return errors.Wrap(err, "can not delete replyMessages")
        }
        // 削除処理の結果から削除されたレコード数を取得
        rowsAffected, err := res.RowsAffected()
        if err != nil {
                return errors.Wrap(err, "can not get rowsAffected of replyMessage")
        }
	if d.verbose {
		log.Printf("delete replyMessages (videoId = %v, rowsAffected = %v)", videoId, rowsAffected)
	}

        return nil
}

func (d *DatabaseOperator) DeleteTopLevelMessagesByVideoId(videoId string) (error) {
	res, err := d.db.Exec(`DELETE FROM topLevelMessage WHERE videoId = ?`, videoId)
        if err != nil {
                return errors.Wrap(err, "can not delete topLevelMessages")
        }
        // 削除処理の結果から削除されたレコード数を取得
        rowsAffected, err := res.RowsAffected()
        if err != nil {
                return errors.Wrap(err, "can not get rowsAffected of topLevelMessage")
        }
	if d.verbose {
		log.Printf("delete topLevelMessages (videoId = %v, rowsAffected = %v)", videoId, rowsAffected)
	}

        return nil
}

func (d *DatabaseOperator) DeleteMessageThreadsByVideoId(videoId string) (error) {
	res, err := d.db.Exec(`DELETE FROM commentThread WHERE videoId = ?`, videoId)
        if err != nil {
                return errors.Wrap(err, "can not delete commentThreads")
        }
        // 削除処理の結果から削除されたレコード数を取得
        rowsAffected, err := res.RowsAffected()
        if err != nil {
                return errors.Wrap(err, "can not get rowsAffected of commentThread")
        }
	if d.verbose {
		log.Printf("delete commentThreads (videoId = %v, rowsAffected = %v)", videoId, rowsAffected)
	}

        return nil
}

func (d *DatabaseOperator) updateReplyMessages(tx *sql.Tx, replyMessages []*ReplyMessage) (error) {
	for _, replyMessage := range replyMessages {
		res, err := tx.Exec(
		    `INSERT OR REPLACE INTO replyMessage (
			commentId,
			etag,
			channelId,
			videoId,
			commentThreadId,
			parentId,
			authorChannelUrl,
			authorDisplayName,
			authorProfileImageUrl,
			moderationStatus,
			textDisplay,
			textOriginal,
			publishAt,
			updateAt
		    ) VALUES (
			?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?, ?
		    )`,
		    replyMessage.MessageId,
		    replyMessage.Etag,
		    replyMessage.ChannelId,
		    replyMessage.VideoId,
		    replyMessage.MessageThreadId,
		    replyMessage.ParentId,
		    replyMessage.AuthorChannelUrl,
		    replyMessage.AuthorDisplayName,
		    replyMessage.AuthorProfileImageUrl,
		    replyMessage.ModerationStatus,
		    replyMessage.TextDisplay,
		    replyMessage.TextOriginal,
		    replyMessage.PublishAt,
		    replyMessage.UpdateAt,
		)
		if err != nil {
			return errors.Wrap(err, "can not insert replyMessage")
		}
		// 挿入処理の結果からIDを取得
		id, err := res.LastInsertId()
		if err != nil {
			return errors.Wrap(err, "can not get insert id of replyMessage")
		}
		if d.verbose {
			log.Printf("update reply comment (commentId = %v, insert id = %v)", replyMessage.MessageId, id)
		}
	}
	return nil
}

func (d *DatabaseOperator) updateTopLevelMessage(tx *sql.Tx, topLevelMessage *TopLevelMessage) (error) {
	res, err := tx.Exec(
            `INSERT OR REPLACE INTO topLevelMessage (
                commentId,
                etag,
                channelId,
                videoId,
                commentThreadId,
                authorChannelUrl,
                authorDisplayName,
                authorProfileImageUrl,
                moderationStatus,
                textDisplay,
                textOriginal,
                publishAt,
                updateAt
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?
            )`,
	    topLevelMessage.MessageId,
	    topLevelMessage.Etag,
	    topLevelMessage.ChannelId,
	    topLevelMessage.VideoId,
	    topLevelMessage.MessageThreadId,
	    topLevelMessage.AuthorChannelUrl,
	    topLevelMessage.AuthorDisplayName,
	    topLevelMessage.AuthorProfileImageUrl,
	    topLevelMessage.ModerationStatus,
	    topLevelMessage.TextDisplay,
	    topLevelMessage.TextOriginal,
	    topLevelMessage.PublishAt,
	    topLevelMessage.UpdateAt,
        )
        if err != nil {
                return errors.Wrap(err, "can not insert topLevelMessage")
        }
        // 挿入処理の結果からIDを取得
        id, err := res.LastInsertId()
        if err != nil {
                return errors.Wrap(err, "can not get insert id of topLevelMessage")
        }
	if d.verbose {
		log.Printf("update top level comment (commentId = %v, insert id = %v)", topLevelMessage.MessageId, id)
	}
        return nil
}

func (d *DatabaseOperator) UpdateMessageThread(commentThread *MessageThread) (error) {
        tx, err := d.db.Begin()
        if err != nil {
                return errors.Wrap(err, "can not start transaction in update reply comment")
        }
        defer func() {
                if p := recover(); p != nil {
                        tx.Rollback()
                        panic(p)
                }
        }()
	res, err := tx.Exec(
            `INSERT OR REPLACE INTO commentThread (
                commentThreadId,
                etag,
                name,
                channelId,
                videoId,
		responseEtag
            ) VALUES (
                ?, ?, ?, ?, ?, ?
            )`,
	    commentThread.MessageThreadId,
	    commentThread.Etag,
	    commentThread.Name,
	    commentThread.ChannelId,
	    commentThread.VideoId,
	    commentThread.ResponseEtag,
        )
        if err != nil {
                tx.Rollback()
                return errors.Wrap(err, "can not insert commentThread")
        }
        // 挿入処理の結果からIDを取得
        id, err := res.LastInsertId()
        if err != nil {
                tx.Rollback()
                return errors.Wrap(err, "can not get insert id of commentThread")
        }
	if d.verbose {
		log.Printf("update comment thread (commentThreadId = %v, insert id = %v)", commentThread.MessageThreadId, id)
	}
	err = d.updateTopLevelMessage(tx, commentThread.TopLevelMessage)
	if err != nil {
                tx.Rollback()
                return errors.Wrap(err, "can not update topLevelMessage")
	}
	err = d.updateReplyMessages(tx, commentThread.ReplyMessages)
	if err != nil {
                tx.Rollback()
                return errors.Wrap(err, "can not update replayMessages")
	}
        tx.Commit()
        return nil
}

func (d *DatabaseOperator) GetAllMessagesByChannelIdAndLikeText(channelId string, likeText string) ([]*CommonMessage, error) {
	commonMessages := make([]*CommonMessage, 0)
        topLevelMessageRows, err := d.db.Query(`SELECT * FROM topLevelMessage Where channelId = ? AND textOriginal like ?`, channelId, likeText)
        if err != nil {
                return nil, errors.Wrap(err, "can not get topLevelMessage by channelId and likeText from database")
        }
        defer topLevelMessageRows.Close()
        for topLevelMessageRows.Next() {
                commonMessage := &CommonMessage{}
                // カーソルから値を取得
                err := topLevelMessageRows.Scan(
                    &commonMessage.MessageId,
                    &commonMessage.Etag,
                    &commonMessage.ChannelId,
                    &commonMessage.VideoId,
                    &commonMessage.MessageThreadId,
                    &commonMessage.AuthorChannelUrl,
                    &commonMessage.AuthorDisplayName,
                    &commonMessage.AuthorProfileImageUrl,
                    &commonMessage.ModerationStatus,
                    &commonMessage.TextDisplay,
                    &commonMessage.TextOriginal,
                    &commonMessage.PublishAt,
                    &commonMessage.UpdateAt,
                )
                if err != nil {
                        return nil, errors.Wrap(err, "can not scan topLevelMessage by channelId and likeText from database")
                }
		commonMessages = append(commonMessages, commonMessage)
        }
        replyMessageRows, err := d.db.Query(`SELECT * FROM replyMessage Where channelId = ? AND textOriginal like ?`, channelId, likeText)
        if err != nil {
                return nil, errors.Wrap(err, "can not get replyMessage by channelId and likeText from database")
        }
        defer replyMessageRows.Close()
        for replyMessageRows.Next() {
                commonMessage := &CommonMessage{}
                // カーソルから値を取得
                err := replyMessageRows.Scan(
                    &commonMessage.MessageId,
                    &commonMessage.Etag,
                    &commonMessage.ChannelId,
                    &commonMessage.VideoId,
                    &commonMessage.MessageThreadId,
                    &commonMessage.ParentId,
                    &commonMessage.AuthorChannelUrl,
                    &commonMessage.AuthorDisplayName,
                    &commonMessage.AuthorProfileImageUrl,
                    &commonMessage.ModerationStatus,
                    &commonMessage.TextDisplay,
                    &commonMessage.TextOriginal,
                    &commonMessage.PublishAt,
                    &commonMessage.UpdateAt,
                )
                if err != nil {
                        return nil, errors.Wrap(err, "can not scan replyMessage by channelId and likeText from database")
                }
		commonMessages = append(commonMessages, commonMessage)
        }
        return commonMessages, nil
}

func (d *DatabaseOperator) getReplyMessages(commentThreadId string) ([]*ReplyMessage, error) {
        rows, err := d.db.Query(`SELECT * FROM replyMessage WHERE commentThreadId = ?`, commentThreadId)
        if err != nil {
                return nil, errors.Wrap(err, "can not get replyMessage by commentThreadId from database")
        }
        defer rows.Close()
	replyMessages := make([]*ReplyMessage, 0)
        for rows.Next() {
                replyMessage := &ReplyMessage{}
                // カーソルから値を取得
                err := rows.Scan(
                    &replyMessage.MessageId,
                    &replyMessage.Etag,
                    &replyMessage.ChannelId,
                    &replyMessage.VideoId,
                    &replyMessage.MessageThreadId,
                    &replyMessage.ParentId,
                    &replyMessage.AuthorChannelUrl,
                    &replyMessage.AuthorDisplayName,
                    &replyMessage.AuthorProfileImageUrl,
                    &replyMessage.ModerationStatus,
                    &replyMessage.TextDisplay,
                    &replyMessage.TextOriginal,
                    &replyMessage.PublishAt,
                    &replyMessage.UpdateAt,
                )
                if err != nil {
                        return nil, errors.Wrap(err, "can not scan replyMessage by commentThreadId from database")
                }
		replyMessages = append(replyMessages, replyMessage)
        }
        return replyMessages, nil
}

func (d *DatabaseOperator) getTopLevelMessage(commentThreadId string) (*TopLevelMessage, bool, error) {
        rows, err := d.db.Query(`SELECT * FROM topLevelMessage WHERE commentThreadId = ?`, commentThreadId)
        if err != nil {
                return nil, false, errors.Wrap(err, "can not get topLevelMessage by commentThreadId from database")
        }
        defer rows.Close()
        for rows.Next() {
                topLevelMessage := &TopLevelMessage{}
                // カーソルから値を取得
                err := rows.Scan(
                    &topLevelMessage.MessageId,
                    &topLevelMessage.Etag,
                    &topLevelMessage.ChannelId,
                    &topLevelMessage.VideoId,
                    &topLevelMessage.MessageThreadId,
                    &topLevelMessage.AuthorChannelUrl,
                    &topLevelMessage.AuthorDisplayName,
                    &topLevelMessage.AuthorProfileImageUrl,
                    &topLevelMessage.ModerationStatus,
                    &topLevelMessage.TextDisplay,
                    &topLevelMessage.TextOriginal,
                    &topLevelMessage.PublishAt,
                    &topLevelMessage.UpdateAt,
                )
                if err != nil {
                        return nil, false, errors.Wrap(err, "can not scan topLevelMessage by commentThreadId from database")
                }
		return topLevelMessage, true, nil
        }
        return nil, false, nil
}

func (d *DatabaseOperator) GetMessageThreadByMessageThreadId(commentThreadId string) (*MessageThread, bool, error) {
        rows, err := d.db.Query(`SELECT * FROM commentThread WHERE commentThreadId = ?`, commentThreadId)
        if err != nil {
                return nil, false, errors.Wrap(err, "can not get commentThread by commentThreadId from database")
        }
        defer rows.Close()
        for rows.Next() {
                commentThread := &MessageThread{}
                // カーソルから値を取得
                err := rows.Scan(
                    &commentThread.MessageThreadId,
                    &commentThread.Etag,
                    &commentThread.Name,
                    &commentThread.ChannelId,
                    &commentThread.VideoId,
                    &commentThread.ResponseEtag,
                )
                if err != nil {
                        return nil, false, errors.Wrap(err, "can not scan commentThread by commentThreadId from database")
                }
		topLevelMessage, ok, err := d.getTopLevelMessage(commentThread.MessageThreadId)
		if err != nil {
                        return nil, false, errors.Wrap(err, "can not get top level comment by commentThreadId from database")
		}
		if !ok {
                        return nil, false, errors.Wrap(err, "not found top level comment by commentThreadId from database")
		}
		replyMessages, err := d.getReplyMessages(commentThread.MessageThreadId)
		if err != nil {
                        return nil, false, errors.Wrap(err, "can not get reply comments by commentThreadId from database")
		}
		commentThread.TopLevelMessage = topLevelMessage
		commentThread.ReplyMessages = replyMessages
		return commentThread, true, nil
        }
        return nil, false, nil
}

func (d *DatabaseOperator) DeleteVideoByVideoId(videoId string) (error) {
	res, err := d.db.Exec(`DELETE FROM video WHERE videoId = ?`, videoId)
        if err != nil {
                return errors.Wrap(err, "can not delete video")
        }
        // 削除処理の結果から削除されたレコード数を取得
        rowsAffected, err := res.RowsAffected()
        if err != nil {
                return errors.Wrap(err, "can not get rowsAffected of video")
        }
	if d.verbose {
		log.Printf("delete video (videoId = %v, rowsAffected = %v)", videoId, rowsAffected)
	}

        return nil
}

func (d *DatabaseOperator) UpdateVideo(video *Video) (error) {
	res, err := d.db.Exec(
            `INSERT OR REPLACE INTO video (
                videoId,
                etag,
                name,
                channelId,
                channelTitle,
                title,
                description,
                publishdAt,
                duration,
                liveStreamActiveLiveChatId,
                liveStreamActualStartTime,
                liveStreamActualEndTime,
                liveStreamScheduledStartTime,
                liveStreamScheduledEndTime,
                thumbnailDefaultUrl,
                thumbnailDefaultWidth,
                thumbnailDefaultHeight,
                thumbnailHighUrl,
                thumbnailHighWidth,
                thumbnailHighHeight,
                thumbnailMediumUrl,
                thumbnailMediumWidth,
                thumbnailMediumHeight,
                embedHeight,
                embedWidth,
                embedHtml,
                statusPrivacyStatus,
                statusUploadStatus,
                statusEmbeddable,
		responseEtag
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )`,
	    video.VideoId,
	    video.Etag,
	    video.Name,
	    video.ChannelId,
	    video.ChannelTitle,
	    video.Title,
	    video.Description,
	    video.PublishdAt,
	    video.Duration,
            video.LiveStreamActiveLiveChatId,
            video.LiveStreamActualStartTime,
            video.LiveStreamActualEndTime,
            video.LiveStreamScheduledStartTime,
            video.LiveStreamScheduledEndTime,
	    video.ThumbnailDefaultUrl,
	    video.ThumbnailDefaultWidth,
	    video.ThumbnailDefaultHeight,
	    video.ThumbnailHighUrl,
	    video.ThumbnailHighWidth,
	    video.ThumbnailHighHeight,
	    video.ThumbnailMediumUrl,
	    video.ThumbnailMediumWidth,
	    video.ThumbnailMediumHeight,
	    video.EmbedHeight,
	    video.EmbedWidth,
	    video.EmbedHtml,
	    video.StatusPrivacyStatus,
	    video.StatusUploadStatus,
            video.StatusEmbeddable,
	    video.ResponseEtag,
        )
        if err != nil {
                return errors.Wrap(err, "can not insert video")
        }
        // 挿入処理の結果からIDを取得
        id, err := res.LastInsertId()
        if err != nil {
                return errors.Wrap(err, "can not get insert id of video")
        }
	if d.verbose {
		log.Printf("update video (videoId = %v, insert id = %v)", video.VideoId, id)
	}

        return nil
}

func (d *DatabaseOperator) GetOldVideosByChannelIdAndOffset(channelId string, offset int64) ([]*Video, error) {
        rows, err := d.db.Query(`select * from video where channelId = ? order by publishdAt desc limit ?,(select count(videoId) from video where channelId = ?);`, channelId, offset, channelId)
        if err != nil {
                return nil, errors.Wrap(err, "can not get videos from database")
        }
        defer rows.Close()
	videos := make([]*Video, 0)
        for rows.Next() {
                video := &Video{}
                // カーソルから値を取得
                err := rows.Scan(
                    &video.VideoId,
                    &video.Etag,
                    &video.Name,
                    &video.ChannelId,
                    &video.ChannelTitle,
                    &video.Title,
                    &video.Description,
                    &video.PublishdAt,
                    &video.Duration,
                    &video.LiveStreamActiveLiveChatId,
                    &video.LiveStreamActualStartTime,
                    &video.LiveStreamActualEndTime,
                    &video.LiveStreamScheduledStartTime,
                    &video.LiveStreamScheduledEndTime,
                    &video.ThumbnailDefaultUrl,
                    &video.ThumbnailDefaultWidth,
                    &video.ThumbnailDefaultHeight,
                    &video.ThumbnailHighUrl,
                    &video.ThumbnailHighWidth,
                    &video.ThumbnailHighHeight,
                    &video.ThumbnailMediumUrl,
                    &video.ThumbnailMediumWidth,
                    &video.ThumbnailMediumHeight,
		    &video.EmbedHeight,
		    &video.EmbedWidth,
		    &video.EmbedHtml,
                    &video.StatusPrivacyStatus,
                    &video.StatusUploadStatus,
                    &video.StatusEmbeddable,
		    &video.ResponseEtag,
                )
                if err != nil {
                        return nil, errors.Wrap(err, "can not scan videos from database")
                }
		videos = append(videos, video)
        }
        return videos, nil
}

func (d *DatabaseOperator) GetVideosByChannelId(channelId string) ([]*Video, error) {
        rows, err := d.db.Query(`SELECT * FROM video WHERE channelId = ?`, channelId)
        if err != nil {
                return nil, errors.Wrap(err, "can not get videos from database")
        }
        defer rows.Close()
	videos := make([]*Video, 0)
        for rows.Next() {
                video := &Video{}
                // カーソルから値を取得
                err := rows.Scan(
                    &video.VideoId,
                    &video.Etag,
                    &video.Name,
                    &video.ChannelId,
                    &video.ChannelTitle,
                    &video.Title,
                    &video.Description,
                    &video.PublishdAt,
                    &video.Duration,
                    &video.LiveStreamActiveLiveChatId,
                    &video.LiveStreamActualStartTime,
                    &video.LiveStreamActualEndTime,
                    &video.LiveStreamScheduledStartTime,
                    &video.LiveStreamScheduledEndTime,
                    &video.ThumbnailDefaultUrl,
                    &video.ThumbnailDefaultWidth,
                    &video.ThumbnailDefaultHeight,
                    &video.ThumbnailHighUrl,
                    &video.ThumbnailHighWidth,
                    &video.ThumbnailHighHeight,
                    &video.ThumbnailMediumUrl,
                    &video.ThumbnailMediumWidth,
                    &video.ThumbnailMediumHeight,
		    &video.EmbedHeight,
		    &video.EmbedWidth,
		    &video.EmbedHtml,
                    &video.StatusPrivacyStatus,
                    &video.StatusUploadStatus,
                    &video.StatusEmbeddable,
		    &video.ResponseEtag,
                )
                if err != nil {
                        return nil, errors.Wrap(err, "can not scan videos from database")
                }
		videos = append(videos, video)
        }
        return videos, nil
}

func (d *DatabaseOperator) GetVideoByVideoId(videoId string) (*Video, bool, error) {
        rows, err := d.db.Query(`SELECT * FROM video WHERE videoId = ?`, videoId)
        if err != nil {
                return nil, false, errors.Wrap(err, "can not get video by videoId from database")
        }
        defer rows.Close()
        for rows.Next() {
                video := &Video{}
                // カーソルから値を取得
                err := rows.Scan(
                    &video.VideoId,
                    &video.Etag,
                    &video.Name,
                    &video.ChannelId,
                    &video.ChannelTitle,
                    &video.Title,
                    &video.Description,
                    &video.PublishdAt,
                    &video.Duration,
                    &video.LiveStreamActiveLiveChatId,
                    &video.LiveStreamActualStartTime,
                    &video.LiveStreamActualEndTime,
                    &video.LiveStreamScheduledStartTime,
                    &video.LiveStreamScheduledEndTime,
                    &video.ThumbnailDefaultUrl,
                    &video.ThumbnailDefaultWidth,
                    &video.ThumbnailDefaultHeight,
                    &video.ThumbnailHighUrl,
                    &video.ThumbnailHighWidth,
                    &video.ThumbnailHighHeight,
                    &video.ThumbnailMediumUrl,
                    &video.ThumbnailMediumWidth,
                    &video.ThumbnailMediumHeight,
		    &video.EmbedHeight,
		    &video.EmbedWidth,
		    &video.EmbedHtml,
                    &video.StatusPrivacyStatus,
                    &video.StatusUploadStatus,
                    &video.StatusEmbeddable,
		    &video.ResponseEtag,
                )
                if err != nil {
                        return nil, false, errors.Wrap(err, "can not scan video by videoId from database")
                }
		return video, true, nil
        }
        return nil, false, nil
}

func (d *DatabaseOperator) UpdateChannel(channel *Channel) (error) {
	res, err := d.db.Exec(
            `INSERT OR REPLACE INTO channel (
                channelId,
                etag,
                name,
		customUrl,
                title,
                description,
                publishdAt,
                thumbnailDefaultUrl,
                thumbnailDefaultWidth,
                thumbnailDefaultHeight,
                thumbnailHighUrl,
                thumbnailHighWidth,
                thumbnailHighHeight,
                thumbnailMediumUrl,
                thumbnailMediumWidth,
                thumbnailMediumHeight,
		responseEtag,
		twitterName
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?
            )`,
	    channel.ChannelId,
	    channel.Etag,
	    channel.Name,
	    channel.CustomUrl,
	    channel.Title,
	    channel.Description,
	    channel.PublishdAt,
	    channel.ThumbnailDefaultUrl,
	    channel.ThumbnailDefaultWidth,
	    channel.ThumbnailDefaultHeight,
	    channel.ThumbnailHighUrl,
	    channel.ThumbnailHighWidth,
	    channel.ThumbnailHighHeight,
	    channel.ThumbnailMediumUrl,
	    channel.ThumbnailMediumWidth,
	    channel.ThumbnailMediumHeight,
	    channel.ResponseEtag,
            channel.TwitterName,
        )
        if err != nil {
                return errors.Wrap(err, "can not insert channel")
        }
        // 挿入処理の結果からIDを取得
        id, err := res.LastInsertId()
        if err != nil {
                return errors.Wrap(err, "can not get insert id of channel")
        }
	if d.verbose {
		log.Printf("update channel (channelId = %v, insert id = %v)", channel.ChannelId, id)
	}

        return nil
}

func (d *DatabaseOperator) GetChannelByChannelId(channelId string) (*Channel, bool, error) {
        rows, err := d.db.Query(`SELECT * FROM channel WHERE channelId = ?`, channelId)
        if err != nil {
                return nil, false, errors.Wrap(err, "can not get videos from database")
        }
        defer rows.Close()
        for rows.Next() {
                channel := &Channel{}
                // カーソルから値を取得
                err := rows.Scan(
                    &channel.ChannelId,
                    &channel.Etag,
                    &channel.Name,
                    &channel.CustomUrl,
                    &channel.Title,
                    &channel.Description,
                    &channel.PublishdAt,
                    &channel.ThumbnailDefaultUrl,
                    &channel.ThumbnailDefaultWidth,
                    &channel.ThumbnailDefaultHeight,
                    &channel.ThumbnailHighUrl,
                    &channel.ThumbnailHighWidth,
                    &channel.ThumbnailHighHeight,
                    &channel.ThumbnailMediumUrl,
                    &channel.ThumbnailMediumWidth,
                    &channel.ThumbnailMediumHeight,
		    &channel.ResponseEtag,
		    &channel.TwitterName,
                )
                if err != nil {
                        return nil, false, errors.Wrap(err, "can not scan channel from database")
                }
		return channel, true, nil
        }
        return nil, false, nil
}

func (d *DatabaseOperator) UpdateSha1DigestAndDirtyOfChannelPage(channelId string, pageHash string, dirty int64, tweetId int64) (error) {
	res, err := d.db.Exec(
            `INSERT OR REPLACE INTO channelPage (
                channelId,
                sha1Digest,
                dirty,
		tweetId
            ) VALUES (
                ?, ?, ?, ?
            )`,
	    channelId,
	    pageHash,
	    dirty,
	    tweetId,
        )
        if err != nil {
                return errors.Wrap(err, "can not insert channelPage")
        }
        // 挿入処理の結果からIDを取得
        id, err := res.LastInsertId()
        if err != nil {
                return errors.Wrap(err, "can not get insert id of channelPage")
        }
	if d.verbose {
		log.Printf("update channel page (channelId = %v, insert id = %v)", channelId, id)
	}

	return nil
}

func (d *DatabaseOperator) UpdateDirtyAndTweetIdOfChannelPage(channelId string, dirty int64, tweetId int64) (error) {
	res, err := d.db.Exec( `UPDATE channelPage SET dirty = ?, tweetId = ? WHERE channelId = ?` , dirty, tweetId, channelId)
        if err != nil {
                return errors.Wrap(err, "can not update channelPage")
        }
        // 更新処理の結果からIDを取得
        rowsAffected, err := res.RowsAffected()
        if err != nil {
                return errors.Wrap(err, "can not get rowsAffected of channelPage")
        }
	if d.verbose {
		log.Printf("update channel page (channelId = %v, rowsAffected = %v)", channelId, rowsAffected)
	}

	return nil
}


func (d *DatabaseOperator) GetChannelPageByChannelId(channelId string) (*ChannelPage, bool, error) {
        rows, err := d.db.Query(`SELECT * FROM channelPage WHERE channelId = ?`, channelId)
        if err != nil {
                return nil, false, errors.Wrap(err, "can not get channelPage by chanelId from database")
        }
        defer rows.Close()
        for rows.Next() {
                channelPage := &ChannelPage{}
                // カーソルから値を取得
                err := rows.Scan(
                    &channelPage.ChannelId,
                    &channelPage.Sha1Digest,
                    &channelPage.Dirty,
		    &channelPage.TweetId,
                )
                if err != nil {
                        return nil, false, errors.Wrap(err, "can not scan channelPage by channelId from database")
                }
		return channelPage, true, nil
        }
        return nil, false, nil

}










func (d *DatabaseOperator) createTables() (error) {
        videoTableCreateQuery := `
            CREATE TABLE IF NOT EXISTS video (
                videoId                      TEXT PRIMARY KEY,
		categoryId                   TEXT NOT NULL
                channelId                    TEXT NOT NULL,
                title                        TEXT NOT NULL,
                description                  TEXT NOT NULL,
		publishdAt                   TEXT NOT NULL,
		duration                     TEXT NOT NULL,
		liveStreamActiveLiveChatId   TEXT NOT NULL,
		liveStreamActualStartTime    TEXT NOT NULL,
		liveStreamActualEndtime      TEXT NOT NULL,
		liveStreamScheduledStartTime TEXT NOT NULL,
		liveStreamScheduledEndTime   TEXT NOT NULL,
		statusPrivacyStatus          TEXT NOT NULL,
		statusUploadStatus           TEXT NOT NULL,
		statusEmbeddable             TEXT NOT NULL,

	)`
	_, err = d.db.Exec(videoTableCreateQuery);
	if  err != nil {
		return errors.Wrap(err, "can not create video table")
	}

        activeLiveChatMessageTableCreateQuery := `
            CREATE TABLE IF NOT EXISTS activeLiveChatMessage (
                messageId                       TEXT PRIMARY KEY,
		channelId                       TEXT NOT NULL,
		videoId                         TEXT NOT NULL,
		apiEtag                         TEXT NOT NULL,
		pageToken                       TEXT NOT NULL,
		nextPageTolken                  TEXT NOT NULL,
		authorChannelId                 TEXT NOT NULL,
		authorChannelUrl                TEXT NOT NULL,
		authorDisplayName               TEXT NOT NULL,
		authorProfileImageUrl           TEXT NOT NULL,
		authorIsChatModerator           TEXT NOT NULL,
		authorIsChatOwner               TEXT NOT NULL,
		authorIsChatSponsor             TEXT NOT NULL,
		authorIsVerified                TEXT NOT NULL,
		liveChatId                      TEXT NOT NULL,
		displayMessage                  TEXT NOT NULL,
		messagePublishedAt              TEXT NOT NULL,
		isSuperChat                     TEXT NOT NULL,
		amountDisplayString             TEXT NOT NULL,
		currency                        TEXT NOT NULL,
	)`
	_, err = d.db.Exec(avtiveLiveChatMessageTableCreateQuery);
	if  err != nil {
		return errors.Wrap(err, "can not create activeLiveChatMessage table")
	}
        liveChatMessageVideoIdIndexQuery := `CREATE INDEX IF NOT EXISTS activeLiveChatMessage_videoId_index ON activeLiveChatMessage(videoId)`
	_, err = d.db.Exec(avtiveLiveChatMessageVideoIdIndexQuery);
	if  err != nil {
		return errors.Wrap(err, "can not create videoId index of avtiveLiveChatMessage")
	}
        liveChatMessageChannelIdIndexQuery := `CREATE INDEX IF NOT EXISTS activeLiveChatMessage_channelId_index ON activeLiveChatMessage(channelId)`
	_, err = d.db.Exec(activeLiveChatMessageChannelIdIndexQuery);
	if  err != nil {
		return errors.Wrap(err, "can not create channelId index of activeLiveChatMessage")
	}

        liveChatMessageTableCreateQuery := `
            CREATE TABLE IF NOT EXISTS liveChatMessage (
		messageId           TEXT PRIMARY KEY,
		channelId           TEXT NOT NULL,
		videoId             TEXT NOT NULL,
		clientId            TEXT NOT NULL,
		timestampAt         TEXT NOT NULL,
		timestampText       TEXT NOT NULL,
		authorName          TEXT NOT NULL,
		authorPhotoUrl      TEXT NOT NULL,
		messageText         TEXT NOT NULL,
		purchaseAmountText  TEXT NOT NULL,
		videoOffsetTimeMsec TEXT NOT NULL
	)`
	_, err = d.db.Exec(liveChatMessageTableCreateQuery);
	if  err != nil {
		return errors.Wrap(err, "can not create liveChatMessage table")
	}
        liveChatMessageVideoIdIndexQuery := `CREATE INDEX IF NOT EXISTS liveChatMessage_videoId_index ON liveChatMessage(videoId)`
	_, err = d.db.Exec(liveChatMessageVideoIdIndexQuery);
	if  err != nil {
		return errors.Wrap(err, "can not create vodeoId index of liveChatMessage")
	}
        liveChatMessageChannelIdIndexQuery := `CREATE INDEX IF NOT EXISTS liveChatMessage_channelId_index ON liveChatMessage(channelId)`
	_, err = d.db.Exec(liveChatMessageChannelIdIndexQuery);
	if  err != nil {
		return errors.Wrap(err, "can not create channelId index liveChatMessage")
	}

	return nil
}

func (d *DatabaseOperator) Open() (error) {
        db, err := sql.Open("sqlite3", d.databasePath)
        if err != nil {
                return errors.Wrapf(err, "can not open database")
        }
        d.db = db
        err = d.createTables()
        if err != nil {
                return errors.Wrapf(err, "can not create table of database")
        }
        return nil
}

func (d *DatabaseOperator) Close()  {
        d.db.Close()
}

func NewDatabaseOperator(databasePath string, verbose bool) (*DatabaseOperator, error) {
        if databasePath == "" {
                return nil, errors.New("no database path")
        }
        dirname := filepath.Dir(databasePath)
        _, err := os.Stat(dirname)
        if err != nil {
                err := os.MkdirAll(dirname, 0755)
                if err != nil {
                        return nil, errors.Errorf("can not create directory (%v)", dirname)
                }
        }
        return &DatabaseOperator{
                databasePath: databasePath,
                db: nil,
		verbose: verbose,
        }, nil
}
