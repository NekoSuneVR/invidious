require "http/client"

module PoToken
  extend self

  # Cached entry returned by the po_token service
  struct Entry
    getter token : String
    getter expires_at : Time
    getter content_binding : String
  end

  CACHE_BUFFER = 30.seconds
  FALLBACK_TTL = 10.minutes

  @@cache = {} of String => Entry
  @@mutex = Mutex.new

  def enabled?
    !CONFIG.po_token_endpoint.to_s.empty?
  end

  def content_binding_for(video_id : String) : String?
    return nil unless enabled?
    template = CONFIG.po_token_binding_template
    return nil if template.empty?
    template.gsub("%{video_id}", video_id)
  end

  def token_for(video_id : String) : String?
    binding = content_binding_for(video_id)
    return nil if binding.nil? || binding.empty?

    if entry = fetch_entry(binding)
      return entry.token
    end

    nil
  end

  private def fetch_entry(binding : String) : Entry?
    now = Time.utc

    @@mutex.synchronize do
      if cached = @@cache[binding]?
        return cached if cached.expires_at - now > CACHE_BUFFER
      end

      entry = request_entry(binding)
      @@cache[binding] = entry if entry
      entry
    end
  end

  private def request_entry(binding : String) : Entry?
    endpoint = build_endpoint
    return nil unless endpoint

    payload = {
      "content_binding"          => binding,
      "proxy"                    => "",
      "bypass_cache"             => false,
      "disable_tls_verification" => false,
    }.to_json

    headers = HTTP::Headers{
      "Content-Type" => "application/json",
      "Accept"       => "application/json",
    }

    begin
      response = HTTP::Client.post(endpoint, headers: headers, body: payload)
    rescue ex
      LOGGER.warn("PoToken: failed to contact service: #{ex.message}")
      return nil
    end

    if response.status_code < 200 || response.status_code >= 300
      LOGGER.warn("PoToken: service returned status #{response.status_code}")
      return nil
    end

    begin
      data = JSON.parse(response.body).as_h
    rescue ex
      LOGGER.warn("PoToken: failed to parse response: #{ex.message}")
      return nil
    end

    token = data["poToken"]?.try &.as_s
    return nil if token.nil? || token.empty?

    expires_at = data["expiresAt"]?.try do |value|
      Time.parse_rfc3339(value.as_s)
    rescue
      nil
    end
    expires_at ||= Time.utc + FALLBACK_TTL

    content_binding = data["contentBinding"]?.try &.as_s || binding

    Entry.new(
      token: token,
      expires_at: expires_at,
      content_binding: content_binding,
    )
  rescue ex
    LOGGER.warn("PoToken: unexpected error while requesting token: #{ex.message}")
    nil
  end

  private def build_endpoint : String?
    return nil unless enabled?
    url = CONFIG.po_token_endpoint
    return nil if url.to_s.empty?

    # If the configured endpoint already includes a path, use it as-is.
    # Otherwise, default to /get_pot.
    return url.to_s unless url.path.empty? || url.path == "/"

    uri = url.dup
    uri.path = "/get_pot"
    uri.to_s
  end
end
