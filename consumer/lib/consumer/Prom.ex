defmodule Prom do
  def metrics do
    Req.get!("http://localhost:9090/metrics").body
  end

  def status do
    Req.get!("http://localhost:9090/status").body
  end
end
