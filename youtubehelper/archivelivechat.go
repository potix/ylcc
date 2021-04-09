package youtubehelper

import (
    "fmt"
    "net/http"
    "io"
    "io/ioutil"
    "regexp"
    "encoding/json"
    "bytes"
    "net/url"
)

const(
        youtubeBaseUrl string = "https://www.youtube.com/watch?v="
        youtubeLiveChatReplayBaseUrl string = "https://www.youtube.com/live_chat_replay?continuation="
        youtubeLiveChatApiBaseUrl string = "https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay?key="
        userAgent string = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.91 Safari/537.36"
)

type ArchiveLiveChatParams map[string]string

func (a ArchiveLiveChatParams) GetContinuation() (string) {
	return a["continuation"]
}

type ArchiveLiveChatCollector struct {
	verbose bool
	res     map[string]*regexp.Regexp
}

func (a *ArchiveLiveChatCollector) httpRequest(url string, method string, header map[string]string, reqBody io.Reader) ([]byte, error) {
        req, err := http.NewRequest(method, url, reqBody)
        if err != nil {
                return nil, fmt.Errorf("can not create http request (url = %v): %w", url, err)
        }
	for k, v := range header {
		req.Header.Set(k, v)
	}
        client := new(http.Client)
        resp, err := client.Do(req)
        if err != nil {
                return nil, fmt.Errorf("can not request of http (url = %v): %w", url, err)
        }
        defer resp.Body.Close()
        if resp.StatusCode != 200 {
                return nil, fmt.Errorf("response have unexpected status (url = %v, status = %v)", url, resp.Status)
        }
        respBody, err := ioutil.ReadAll(resp.Body)
        if err != nil {
                return nil, fmt.Errorf("can not read body (url = %v): %w", url, err)
        }
        return respBody, nil
}

func (a *ArchiveLiveChatCollector)getParam(re *regexp.Regexp, body []byte) (string, error) {
	v := re.FindAllSubmatch(body, -1)
	if len(v) == 0 {
		return "", fmt.Errorf("not found parameter '%v'", re.String())
	}
	if len(v[0]) <= 1 {
		return "", fmt.Errorf("not found parameter '%v'", re.String())
	}
	return string(v[0][1]), nil
}

func (a *ArchiveLiveChatCollector) GetParams(videoId string) (ArchiveLiveChatParams, error) {
	url := youtubeBaseUrl + videoId
	header := make(map[string]string)
	header["User-Agent"] = userAgent
	body, err := a.httpRequest(url, "GET", header, nil)
	if err != nil {
		return nil, fmt.Errorf("can not get video page (url = %v,  heade = %+v): %v", url, header, err)
	}
	params := make(map[string]string)
	params["offsetMs"] = "0"
	for name, re := range a.res {
		v, err := a.getParam(re, body)
		if err != nil {
			return nil, fmt.Errorf("can not get param (name = %v): %v", name, err)
		}
		params[name] = v
	}
	return params, nil
}

func (a *ArchiveLiveChatCollector) buildRequestBody(params ArchiveLiveChatParams) (*bytes.Buffer, error) {
        request := &GetLiveChatRequest{}
        request.Context.Client.Hl = params["hl"]
        request.Context.Client.Gl = params["gl"]
        request.Context.Client.RemoteHost = params["remoteHost"]
        request.Context.Client.DeviceMake = params["deviceMake"]
        request.Context.Client.DeviceModel = params["deviceModel"]
        request.Context.Client.VisitorData = params["visitorData"]
        request.Context.Client.UserAgent = params["userAgent"]
        request.Context.Client.ClientName = params["clientName"]
        request.Context.Client.ClientVersion = params["clientVersion"]
        request.Context.Client.OsName = params["osName"]
        request.Context.Client.OsVersion = params["osVersion"]
        request.Context.Client.OriginalURL = youtubeLiveChatReplayBaseUrl + url.QueryEscape(params["continuation"])
        request.Context.Client.Platform = params["platform"]
        request.Context.Client.ClientFormFactor = params["clientFormFactor"]
        request.Context.Client.TimeZone = "Asia/Tokyo"
        request.Context.Client.BrowserName = params["browserName"]
        request.Context.Client.BrowserVersion = params["browserVersion"]
        request.Context.Client.UtcOffsetMinutes = 540
        request.Context.Client.MainAppWebInfo.GraftURL = youtubeLiveChatReplayBaseUrl + url.QueryEscape(params["continuation"])
        request.Context.Client.MainAppWebInfo.WebDisplayMode = "WEB_DISPLAY_MODE_BROWSER"
        request.Context.User.LockedSafetyMode = false
        request.Context.Request.UseSsl = true
        request.Continuation = params["continuation"]
        request.CurrentPlayerState.PlayerOffsetMs = params["offsetMs"]
        requestBytes, err := json.Marshal(request)
        if err != nil {
                return nil, fmt.Errorf("can not convert struct to json: %v", err)
        }
        fmt.Printf("%v\n", string(requestBytes))
        return bytes.NewBuffer(requestBytes), nil
}

func (a *ArchiveLiveChatCollector) GetArchiveLiveChat(params ArchiveLiveChatParams) (*GetLiveChatRespose, error) {
	reqBody, err := a.buildRequestBody(params)
	if err != nil {
		return nil, fmt.Errorf("can not build request body: %v", err)
	}
	url := youtubeLiveChatApiBaseUrl + params["innertubeApiKey"]
	header := make(map[string]string)
	header["User-Agent"] = userAgent
	header["Content-Type"] = "application/json"
	respBody, err := a.httpRequest(url, "POST", header, reqBody)
	if err != nil {
		return nil, fmt.Errorf("can not get archive live chat (url = %v, header = %+v, request =%v): %v", url, header, string(reqBody.Bytes()), err)
	}
	resp := &GetLiveChatRespose{}
	if err = json.Unmarshal(respBody, resp); err != nil {
		return nil, fmt.Errorf("can not convert json to struct: %v", err)
	}
	return resp, nil
}

func (a *ArchiveLiveChatCollector) Next(params ArchiveLiveChatParams, resp *GetLiveChatRespose) (bool) {
	params["continuation"] = ""
	for _, c := range resp.ContinuationContents.LiveChatContinuation.Continuations {
                if c.LiveChatReplayContinuationData.Continuation != ""  {
                        params["continuation"] = c.LiveChatReplayContinuationData.Continuation
                }
        }
        for i := len(resp.ContinuationContents.LiveChatContinuation.Actions) - 1; i > 0; i-- {
                if resp.ContinuationContents.LiveChatContinuation.Actions[i].ReplayChatItemAction.VideoOffsetTimeMsec != "" {
                        params["offsetMs"] = resp.ContinuationContents.LiveChatContinuation.Actions[i].ReplayChatItemAction.VideoOffsetTimeMsec
                        break
                }
        }
	if params["continuation"] == "" {
		return false
	} else {
		return true
	}
}

func NewArchiveLiveChatCollector(opts ...Option) (*ArchiveLiveChatCollector) {
	baseOpts := defaultOptions()
        for _, opt := range opts {
                opt(baseOpts)
        }
	res := make(map[string]*regexp.Regexp)
	res["continuation"] = regexp.MustCompile(`"liveChatRenderer".+?"continuations".+?"reloadContinuationData".+?"continuation"[ ]*:[ ]*"([^"]+)"`)
	res["visitorData"] = regexp.MustCompile(`"visitorData"[ ]*:[ ]*"([^"]+)"`)
	res["innertubeApiKey"] = regexp.MustCompile(`"innertubeApiKey"[ ]*:[ ]*"([^"]+)"`)
	res["browserName"] = regexp.MustCompile(`"browserName"[ ]*:[ ]*"([^"]+)"`)
	res["browserVersion"] = regexp.MustCompile(`"browserVersion"[ ]*:[ ]*"([^"]+)"`)
	res["clientName"] = regexp.MustCompile(`"clientName"[ ]*:[ ]*"([^"]+)"`)
	res["clientVersion"] = regexp.MustCompile(`"clientVersion"[ ]*:[ ]*"([^"]+)"`)
	res["remoteHost"] = regexp.MustCompile(`"remoteHost"[ ]*:[ ]*"([^"]+)"`)
	res["gl"] = regexp.MustCompile(`"GL"[ ]*:[ ]*"([^"]+)"`)
	res["hl"] = regexp.MustCompile(`"HL"[ ]*:[ ]*"([^"]+)"`)
	res["osName"] = regexp.MustCompile(`"osName"[ ]*:[ ]*"([^"]+)"`)
	res["osVersion"] = regexp.MustCompile(`"osVersion"[ ]*:[ ]*"([^"]+)"`)
	res["deviceMake"] = regexp.MustCompile(`"deviceMake"[ ]*:[ ]*"([^"]*)"`)
	res["deviceModel"] = regexp.MustCompile(`"deviceModel"[ ]*:[ ]*"([^"]*)"`)
	res["userAgent"] = regexp.MustCompile(`"userAgent"[ ]*:[ ]*"([^"]+)"`)
	res["platform"] = regexp.MustCompile(`"platform"[ ]*:[ ]*"([^"]+)"`)
	res["clientFormFactor"] = regexp.MustCompile(`"clientFormFactor"[ ]*:[ ]*"([^"]+)"`)
	return &ArchiveLiveChatCollector{
		verbose: baseOpts.verbose,
		res: res,
	}
}

type GetLiveChatRequest struct {
	Context struct {
		AdSignalsInfo struct {
			Params []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"params,omitempty"`
		} `json:"adSignalsInfo"`
		Client struct {
			BrowserName      string `json:"browserName"`
			BrowserVersion   string `json:"browserVersion"`
			ClientFormFactor string `json:"clientFormFactor"`
			ClientName       string `json:"clientName"`
			ClientVersion    string `json:"clientVersion"`
			ConnectionType   string `json:"connectionType,omitempty"`
			DeviceMake       string `json:"deviceMake"`
			DeviceModel      string `json:"deviceModel"`
			Gl               string `json:"gl"`
			Hl               string `json:"hl"`
			MainAppWebInfo   struct {
				GraftURL       string `json:"graftUrl"`
				WebDisplayMode string `json:"webDisplayMode"`
			} `json:"mainAppWebInfo"`
			OriginalURL        string `json:"originalUrl"`
			OsName             string `json:"osName"`
			OsVersion          string `json:"osVersion"`
			Platform           string `json:"platform"`
			RemoteHost         string `json:"remoteHost"`
			ScreenDensityFloat int64  `json:"screenDensityFloat,omitempty"`
			ScreenHeightPoints int64  `json:"screenHeightPoints,omitempty"`
			ScreenPixelDensity int64  `json:"screenPixelDensity,omitempty"`
			ScreenWidthPoints  int64  `json:"screenWidthPoints,omitempty"`
			TimeZone           string `json:"timeZone"`
			UserAgent          string `json:"userAgent"`
			UserInterfaceTheme string `json:"userInterfaceTheme,omitempty"`
			UtcOffsetMinutes   int64  `json:"utcOffsetMinutes"`
			VisitorData        string `json:"visitorData"`
		} `json:"client"`
		ClientScreenNonce string `json:"clientScreenNonce,omitempty"`
		Request           struct {
			ConsistencyTokenJars    []interface{} `json:"consistencyTokenJars,omitempty"`
			InternalExperimentFlags []interface{} `json:"internalExperimentFlags,,omitempty"`
			UseSsl                  bool          `json:"useSsl"`
		} `json:"request"`
		User struct {
			LockedSafetyMode bool `json:"lockedSafetyMode"`
		} `json:"user"`
	} `json:"context"`
	Continuation       string `json:"continuation"`
	CurrentPlayerState struct {
		PlayerOffsetMs string `json:"playerOffsetMs"`
	} `json:"currentPlayerState"`
}

type GetLiveChatRespose struct {
	ContinuationContents struct {
		LiveChatContinuation struct {
			Actions []struct {
				ReplayChatItemAction struct {
					Actions []struct {
						AddChatItemAction struct {
							ClientID string `json:"clientId"`
							Item     struct {
								LiveChatPaidMessageRenderer struct {
									AuthorExternalChannelID string `json:"authorExternalChannelId"`
									AuthorName              struct {
										SimpleText string `json:"simpleText"`
									} `json:"authorName"`
									AuthorNameTextColor int64 `json:"authorNameTextColor"`
									AuthorPhoto         struct {
										Thumbnails []struct {
											Height int64  `json:"height"`
											URL    string `json:"url"`
											Width  int64  `json:"width"`
										} `json:"thumbnails"`
									} `json:"authorPhoto"`
									BodyBackgroundColor      int64 `json:"bodyBackgroundColor"`
									BodyTextColor            int64 `json:"bodyTextColor"`
									ContextMenuAccessibility struct {
										AccessibilityData struct {
											Label string `json:"label"`
										} `json:"accessibilityData"`
									} `json:"contextMenuAccessibility"`
									ContextMenuEndpoint struct {
										CommandMetadata struct {
											WebCommandMetadata struct {
												IgnoreNavigation bool `json:"ignoreNavigation"`
											} `json:"webCommandMetadata"`
										} `json:"commandMetadata"`
										LiveChatItemContextMenuEndpoint struct {
											Params string `json:"params"`
										} `json:"liveChatItemContextMenuEndpoint"`
									} `json:"contextMenuEndpoint"`
									HeaderBackgroundColor int64  `json:"headerBackgroundColor"`
									HeaderTextColor       int64  `json:"headerTextColor"`
									ID                    string `json:"id"`
									Message               struct {
										Runs []struct {
											Text string `json:"text"`
										} `json:"runs"`
									} `json:"message"`
									PurchaseAmountText struct {
										SimpleText string `json:"simpleText"`
									} `json:"purchaseAmountText"`
									TimestampColor int64 `json:"timestampColor"`
									TimestampText  struct {
										SimpleText string `json:"simpleText"`
									} `json:"timestampText"`
									TimestampUsec string `json:"timestampUsec"`
								} `json:"liveChatPaidMessageRenderer"`
								LiveChatTextMessageRenderer struct {
									AuthorBadges []struct {
										LiveChatAuthorBadgeRenderer struct {
											Accessibility struct {
												AccessibilityData struct {
													Label string `json:"label"`
												} `json:"accessibilityData"`
											} `json:"accessibility"`
											CustomThumbnail struct {
												Thumbnails []struct {
													URL string `json:"url"`
												} `json:"thumbnails"`
											} `json:"customThumbnail"`
											Tooltip string `json:"tooltip"`
										} `json:"liveChatAuthorBadgeRenderer"`
									} `json:"authorBadges"`
									AuthorExternalChannelID string `json:"authorExternalChannelId"`
									AuthorName              struct {
										SimpleText string `json:"simpleText"`
									} `json:"authorName"`
									AuthorPhoto struct {
										Thumbnails []struct {
											Height int64  `json:"height"`
											URL    string `json:"url"`
											Width  int64  `json:"width"`
										} `json:"thumbnails"`
									} `json:"authorPhoto"`
									ContextMenuAccessibility struct {
										AccessibilityData struct {
											Label string `json:"label"`
										} `json:"accessibilityData"`
									} `json:"contextMenuAccessibility"`
									ContextMenuEndpoint struct {
										CommandMetadata struct {
											WebCommandMetadata struct {
												IgnoreNavigation bool `json:"ignoreNavigation"`
											} `json:"webCommandMetadata"`
										} `json:"commandMetadata"`
										LiveChatItemContextMenuEndpoint struct {
											Params string `json:"params"`
										} `json:"liveChatItemContextMenuEndpoint"`
									} `json:"contextMenuEndpoint"`
									ID      string `json:"id"`
									Message struct {
										Runs []struct {
											Emoji struct {
												EmojiID string `json:"emojiId"`
												Image   struct {
													Accessibility struct {
														AccessibilityData struct {
															Label string `json:"label"`
														} `json:"accessibilityData"`
													} `json:"accessibility"`
													Thumbnails []struct {
														Height int64  `json:"height"`
														URL    string `json:"url"`
														Width  int64  `json:"width"`
													} `json:"thumbnails"`
												} `json:"image"`
												IsCustomEmoji bool     `json:"isCustomEmoji"`
												SearchTerms   []string `json:"searchTerms"`
												Shortcuts     []string `json:"shortcuts"`
											} `json:"emoji"`
											Text string `json:"text"`
										} `json:"runs"`
									} `json:"message"`
									TimestampText struct {
										SimpleText string `json:"simpleText"`
									} `json:"timestampText"`
									TimestampUsec string `json:"timestampUsec"`
								} `json:"liveChatTextMessageRenderer"`
							} `json:"item"`
						} `json:"addChatItemAction"`
						AddLiveChatTickerItemAction struct {
							DurationSec string `json:"durationSec"`
							Item        struct {
								LiveChatTickerPaidMessageItemRenderer struct {
									Amount struct {
										SimpleText string `json:"simpleText"`
									} `json:"amount"`
									AmountTextColor         int64  `json:"amountTextColor"`
									AuthorExternalChannelID string `json:"authorExternalChannelId"`
									AuthorPhoto             struct {
										Accessibility struct {
											AccessibilityData struct {
												Label string `json:"label"`
											} `json:"accessibilityData"`
										} `json:"accessibility"`
										Thumbnails []struct {
											Height int64  `json:"height"`
											URL    string `json:"url"`
											Width  int64  `json:"width"`
										} `json:"thumbnails"`
									} `json:"authorPhoto"`
									DurationSec        int64  `json:"durationSec"`
									EndBackgroundColor int64  `json:"endBackgroundColor"`
									FullDurationSec    int64  `json:"fullDurationSec"`
									ID                 string `json:"id"`
									ShowItemEndpoint   struct {
										CommandMetadata struct {
											WebCommandMetadata struct {
												IgnoreNavigation bool `json:"ignoreNavigation"`
											} `json:"webCommandMetadata"`
										} `json:"commandMetadata"`
										ShowLiveChatItemEndpoint struct {
											Renderer struct {
												LiveChatPaidMessageRenderer struct {
													AuthorExternalChannelID string `json:"authorExternalChannelId"`
													AuthorName              struct {
														SimpleText string `json:"simpleText"`
													} `json:"authorName"`
													AuthorNameTextColor int64 `json:"authorNameTextColor"`
													AuthorPhoto         struct {
														Thumbnails []struct {
															Height int64  `json:"height"`
															URL    string `json:"url"`
															Width  int64  `json:"width"`
														} `json:"thumbnails"`
													} `json:"authorPhoto"`
													BodyBackgroundColor      int64 `json:"bodyBackgroundColor"`
													BodyTextColor            int64 `json:"bodyTextColor"`
													ContextMenuAccessibility struct {
														AccessibilityData struct {
															Label string `json:"label"`
														} `json:"accessibilityData"`
													} `json:"contextMenuAccessibility"`
													ContextMenuEndpoint struct {
														CommandMetadata struct {
															WebCommandMetadata struct {
																IgnoreNavigation bool `json:"ignoreNavigation"`
															} `json:"webCommandMetadata"`
														} `json:"commandMetadata"`
														LiveChatItemContextMenuEndpoint struct {
															Params string `json:"params"`
														} `json:"liveChatItemContextMenuEndpoint"`
													} `json:"contextMenuEndpoint"`
													HeaderBackgroundColor int64  `json:"headerBackgroundColor"`
													HeaderTextColor       int64  `json:"headerTextColor"`
													ID                    string `json:"id"`
													Message               struct {
														Runs []struct {
															Text string `json:"text"`
														} `json:"runs"`
													} `json:"message"`
													PurchaseAmountText struct {
														SimpleText string `json:"simpleText"`
													} `json:"purchaseAmountText"`
													TimestampColor int64 `json:"timestampColor"`
													TimestampText  struct {
														SimpleText string `json:"simpleText"`
													} `json:"timestampText"`
													TimestampUsec string `json:"timestampUsec"`
												} `json:"liveChatPaidMessageRenderer"`
											} `json:"renderer"`
										} `json:"showLiveChatItemEndpoint"`
									} `json:"showItemEndpoint"`
									StartBackgroundColor int64 `json:"startBackgroundColor"`
								} `json:"liveChatTickerPaidMessageItemRenderer"`
							} `json:"item"`
						} `json:"addLiveChatTickerItemAction"`
					} `json:"actions"`
					VideoOffsetTimeMsec string `json:"videoOffsetTimeMsec"`
				} `json:"replayChatItemAction"`
			} `json:"actions"`
			Continuations []struct {
				LiveChatReplayContinuationData struct {
					Continuation             string `json:"continuation"`
					TimeUntilLastMessageMsec int64  `json:"timeUntilLastMessageMsec"`
				} `json:"liveChatReplayContinuationData"`
				PlayerSeekContinuationData struct {
					Continuation string `json:"continuation"`
				} `json:"playerSeekContinuationData"`
			} `json:"continuations"`
		} `json:"liveChatContinuation"`
	} `json:"continuationContents"`
	ResponseContext struct {
		MainAppWebResponseContext struct {
			DatasyncID string `json:"datasyncId"`
			LoggedOut  bool   `json:"loggedOut"`
		} `json:"mainAppWebResponseContext"`
		ServiceTrackingParams []struct {
			Params []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"params"`
			Service string `json:"service"`
		} `json:"serviceTrackingParams"`
		WebResponseContextExtensionData struct {
			HasDecorated bool `json:"hasDecorated"`
		} `json:"webResponseContextExtensionData"`
	} `json:"responseContext"`
}

