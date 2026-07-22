package sbi

func (s *Server) getCollectorRoutes() []Route {
	return []Route{
		{
			Name:    "AdrfRetrievalNotify",
			Method:  "POST",
			Pattern: "/retrieval-notify",
			APIFunc: s.HandleAdrfRetrievalNotify,
		},
	}
}
