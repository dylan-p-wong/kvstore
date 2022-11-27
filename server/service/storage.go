package service

func (s *server) restoreFromStorage() {
	_, err := s.storage.Get("current_term")
	if err != nil {

	}
	_, err = s.storage.Get("voted_for")
	if err != nil {

	}
	// TODO: load log. snapshots?
}
