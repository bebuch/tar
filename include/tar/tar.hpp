//-----------------------------------------------------------------------------
// Copyright (c) 2013-2018 Benjamin Buch
//
// https://github.com/bebuch/tar
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at https://www.boost.org/LICENSE_1_0.txt)
//-----------------------------------------------------------------------------
#ifndef _tar__tar__hpp_INCLUDED_
#define _tar__tar__hpp_INCLUDED_

#include <io_tools/isubstream.hpp>
#include <io_tools/make_string.hpp>
#include <io_tools/mask_non_print.hpp>

#include <functional>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <utility>
#include <string>
#include <vector>
#include <memory>
#include <array>
#include <tuple>
#include <map>
#include <set>
#include <array>
#include <cassert>

namespace tar{


	namespace impl{ namespace tar{


		using index_t = std::string::difference_type;

		template < index_t ... I >
		using index_sequence_t = std::integer_sequence< index_t, I ... >;

		using field_size_t = index_sequence_t<
			100, 8, 8, 8, 12, 12, 8, 1, 100, 6, 2, 32, 32, 8, 8, 155, 12
		>;


		template < typename IntegerSequence, index_t X >
		struct get_integer;

		template < index_t H, index_t ... I, index_t X >
		struct get_integer< index_sequence_t< H, I ... >, X >{
			static constexpr index_t value =
				get_integer< index_sequence_t< I ... >, X - 1 >::value;
		};

		template < index_t H, index_t ... I >
		struct get_integer< index_sequence_t< H, I ... >, index_t(0) >{
			static constexpr index_t value = H;
		};

		template < index_t X >
		struct field_size{
			static constexpr index_t value =
				get_integer< field_size_t, X >::value;
		};


		template < typename IntegerSequence, index_t X >
		struct get_integer_start;

		template < index_t H, index_t ... I, index_t X >
		struct get_integer_start< index_sequence_t< H, I ... >, X >{
			static constexpr index_t value =
				get_integer_start< index_sequence_t< I ... >, X - 1 >::value +
				H;
		};

		template < index_t H, index_t ... I >
		struct get_integer_start< index_sequence_t< H, I ... >, index_t(0) >{
			static constexpr index_t value = 0;
		};

		template < index_t X >
		struct field_start{
			static constexpr index_t value =
				get_integer_start< field_size_t, X >::value;
		};


		struct field_name{
			enum values{
				name,
				mode,
				uid,
				gid,
				size,
				mtime,
				checksum,
				typeflag,
				linkname,
				magic,
				version,
				uname,
				gname,
				devmajor,
				devminor,
				prefix,
				pad
			};
		};


		template < index_t FieldName, typename Container >
		void write(
			std::array< char, 512 >& buffer,
			Container const& container
		){
			static constexpr auto start = field_start< FieldName >::value;
			static constexpr auto size  = field_size< FieldName >::value;

			auto const begin = container.begin();
			auto const end = container.end();

			if(end - begin > size){
				throw std::runtime_error(io_tools::make_string(
					"Tar: field data to long: [", FieldName, "] is: ",
					end - begin, ", max: ", size
				));
			}

			std::copy(begin, end, buffer.begin() + start);
			std::fill(
				buffer.begin() + start + (end - begin),
				buffer.begin() + start + size, 0
			);
		}


		template < index_t FieldName >
		std::string read(std::array< char, 512 > const& buffer){
			static constexpr auto start = field_start< FieldName >::value;
			static constexpr auto size  = field_size< FieldName >::value;

			return std::string(
				buffer.begin() + start,
				buffer.begin() + start + size
			);
		}

		constexpr std::array< char, field_size< field_name::checksum >::value >
			empty_checksum{{' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '}};

		constexpr std::array< char, 5 > magic{{'u', 's', 't', 'a', 'r'}};
		constexpr std::array< char, 6 > mode{{'0', '0', '0', '6', '4', '4'}};
		constexpr std::array< char, 1 > typeflag{{'0'}};

		inline std::string calc_checksum(std::array< char, 512 > buffer){
			write< field_name::checksum >(buffer, empty_checksum);

			unsigned sum = 0;
			for(unsigned i: buffer) sum += i;

			std::ostringstream os;
			os << std::oct << std::setfill('0') << std::setw(6) << sum << '\0'
				<< ' ';

			return os.str();
		}

		inline std::array< char, 512 > make_posix_header(
			std::string const& name,
			std::size_t size
		){
			std::array< char, 512 > buffer{};

			std::ostringstream os;
			os << std::oct << std::setfill('0') << std::setw(11)
				<< std::time(nullptr);
			std::string mtime = os.str();

			write< field_name::magic >(buffer, magic);
			write< field_name::mode >(buffer, mode);
			write< field_name::mtime >(buffer, mtime);
			write< field_name::typeflag >(buffer, typeflag);


			// Set filename
			if(name.size() == 0){
				throw std::runtime_error("Tar: filename is empty");
			}

			if(name.size() >= 100){
				throw std::runtime_error(
					"Tar: filename larger than 99 characters"
				);
			}

			write< field_name::name >(buffer, name);


			// Set size
			os.str("");
			os << std::oct << std::setfill('0') << std::setw(11) << size;
			write< field_name::size >(buffer, os.str());


			// Set checksum
			write< field_name::checksum >(buffer, calc_checksum(buffer));


			return buffer;
		}

		inline std::string cut_null(std::string const& data){
			return data.substr(0, data.find('\0'));
		}

		inline std::tuple< std::string, std::size_t > read_posix_header(
			std::array< char, 512 > const& buffer
		){
			auto const checksum = read< field_name::checksum >(buffer);
			auto const magic = cut_null(read< field_name::magic >(buffer));
			auto const size = read< field_name::size >(buffer);
			auto const filename = cut_null(read< field_name::name >(buffer));

			if(magic != "ustar"){
				throw std::runtime_error(
					"Tar: loaded file without magic 'ustar', magic is: '" +
					io_tools::mask_non_print(magic) + "'"
				);
			}

			if(checksum != calc_checksum(buffer)){
				throw std::runtime_error(
					"Tar: loaded file with wrong checksum"
				);
			}

			return std::make_tuple(
				std::move(filename),
				static_cast< std::size_t >(std::stol(size, 0, 8))
			);
		}


	} }


	/// \brief Write a simple tar file
	class tar_writer{
	public:
		tar_writer(std::string const& filename):
			outfile_(new std::ofstream(
				filename.c_str(),
				std::ios_base::out | std::ios_base::binary
			)),
			out_(*outfile_) {}

		tar_writer(std::ostream& out):
			out_(out) {}

		~tar_writer(){
			static char const dummy[1024] = {0};
			out_.write(dummy, 1024);
		}

		void write(
			std::string const& filename,
			char const* content,
			std::size_t size
		){
			write(filename, [&](std::ostream& os){
				os.write(content, size);
			}, size);
		}

		void write(std::string const& filename, std::string const& content){
			write(filename, content.data(), content.size());
		}

		void write(
			std::string const& filename,
			std::function< void(std::ostream&) > const& writer
		){
			std::ostringstream os(std::ios_base::out | std::ios_base::binary);
			writer(os);
			write(filename, os.str());
		}

		void write(
			std::string const& filename,
			std::function< void(std::ostream&) > const& writer,
			std::size_t size
		){
			if(!filenames_.emplace(filename).second){
				throw std::runtime_error(
					"Duplicate filename in tar-file: " + filename
				);
			}

			auto const header = impl::tar::make_posix_header(filename, size);

			std::size_t end_record_bytes = (512 - (size % 512)) % 512;
			std::vector< char > buffer(end_record_bytes);

			auto start = out_.tellp();
			out_.write(header.data(), header.size());

			writer(out_);
			auto wrote_size =
				static_cast< std::size_t >(out_.tellp() - start) - 512;
			if(wrote_size != size){
				out_.seekp(start);
				throw std::runtime_error(io_tools::make_string(
					"While writing '", filename,
					"' to tar-file: Writer function wrote ", wrote_size,
					" bytes, but ", size, " where expected"
				));
			}

			out_.write(buffer.data(), buffer.size());
		}

	private:
		/// \brief Output file, if the filename constructor have been called
		std::unique_ptr< std::ofstream > outfile_;

		/// \brief Output stream
		std::ostream& out_;

		/// \brief No filename duplicates
		std::set< std::string > filenames_;
	};

	namespace flag {
		using type = uint8_t;

		/// truncate file to its expected size when its size grow during reading
		/// fail othervise
		constexpr type truncate_growing = (1 << 0);

		/// trim missing files to zero in tar
		/// fail othervise
		constexpr type trim_missing = (1 << 1);
	}

	template< typename Logger, typename Fs, std::size_t buffer_size, flag::type tar_flags >
	class tar_buff: public std::streambuf {

		// remaining files
		std::set< std::string > files_;

		static_assert(buffer_size >= 512, "Buffer have to be big enough for tar header");
		std::array< char, buffer_size > buffer_;

		// state of current file
		std::ifstream is_;
		std::string filename_;
		std::size_t padding_bytes_{0};
		// expected size of the file written to the tar header
		std::size_t expected_file_size_{0};
		// how many bytes we already read from input
		std::size_t actual_file_size_{0};

		// Not owning reference to parent stream, used for propagating errors
		std::istream &upper_;

	public:
		tar_buff(const std::set< std::string > &files, std::istream &upper)
			: files_{files}, upper_{upper}
		{
			assert(!files_.empty());
			next_file();
		}

		~tar_buff()
		{ }

	public:
		int underflow() {
			while (this->gptr() == this->egptr()) {
				if (is_.eof()){ // end of current file
					if (padding_bytes_ > 0) { // record padding
						record_padding();
					} else if (files_.empty()) { // tar eof
						upper_.setstate(std::istream::eofbit);
						return std::char_traits<char>::eof();
					} else { // next file
						next_file();
					}
				} else { // continue with current file
					if (!process_file()){
						Logger::error("Error while reading underlying file", filename_);
						upper_.setstate(is_.rdstate());
						return std::char_traits<char>::eof();
					}
				}
			}
			assert(this->gptr() != this->egptr());
			return std::char_traits<char>::to_int_type(*this->gptr());
		}

	private:
		bool process_file(){
			assert(actual_file_size_ <= expected_file_size_);
			if (is_.bad()) {
				Logger::error("I/O error", filename_);
				return false;
			} else if (is_.fail()) {
				Logger::error("Stream error", filename_);
				return false;
			}

			is_.read(buffer_.data(), buffer_.size());
			size_t size = is_.gcount();
			actual_file_size_ += size;

			if (actual_file_size_ > expected_file_size_){
				if ((tar_flags & flag::truncate_growing) != 0) {
					// truncate to expected size
					size_t diff = actual_file_size_ - expected_file_size_;
					assert(diff <= size);
					size -= diff;
					actual_file_size_ = expected_file_size_;
					Logger::warning(
						"File size increased during read, truncate to " + std::to_string(expected_file_size_),
						filename_);
					is_.setstate(std::ios_base::eofbit); // mark as eof
				} else {
					Logger::error(
						"File size increased during read, expected " + std::to_string(expected_file_size_),
						filename_);
					return false;
				}
			}

			if (is_.eof()) {
				Logger::trace("Read " + std::to_string(actual_file_size_) + " bytes", filename_);
				if (actual_file_size_ != expected_file_size_){
					Logger::error("Read file size " + std::to_string(actual_file_size_) + " " +
								  "is different than expected " + std::to_string(expected_file_size_), filename_);
					is_.setstate(std::ios_base::failbit); // logical error
					return false;
				}
			} else if (is_.bad()) {
				Logger::error("I/O error", filename_);
				return false;
			} else if (is_.fail()) {
				Logger::error("Stream error", filename_);
				return false;
			}

			this->setg(this->buffer_.data(), this->buffer_.data(), this->buffer_.data() + size);
			return true;
		}

		void record_padding(){
			assert(padding_bytes_ <= buffer_.size());
			std::fill(buffer_.data(), buffer_.data()+padding_bytes_, 0);
			this->setg(this->buffer_.data(), this->buffer_.data(), this->buffer_.data() + padding_bytes_);
			padding_bytes_ = 0;
		}

		void next_file() {
			// take next file
			auto fit = files_.begin();
			assert(fit != files_.end());
			filename_ = *fit;
			files_.erase(fit); // remove this file from the set

			Logger::trace("Processing", filename_);
			is_ = std::ifstream(filename_, std::ios_base::in | std::iostream::binary);
			std::optional<uintmax_t> size_opt = Fs::file_size(filename_);
			if (size_opt){
				expected_file_size_ = *size_opt;
			} else{
				if ((tar_flags & flag::trim_missing) != 0) {
					Logger::warning("Error getting file size, trim", filename_);
					expected_file_size_ = 0;
					// recover file stream state, mark as end
					is_.setstate(std::ios_base::eofbit);
				} else {
					Logger::error("Error getting file size", filename_);
					is_.setstate(std::ios_base::failbit); // logical error, process_file will check it later
					expected_file_size_ = 0;
				}
			}
			actual_file_size_ = 0;
			padding_bytes_ = (512 - (expected_file_size_ % 512)) % 512;
			auto const header = impl::tar::make_posix_header(filename_, expected_file_size_);
			assert(buffer_.size() >= header.size());
			std::copy(header.data(), header.data() + header.size(), buffer_.data());

			this->setg(this->buffer_.data(), this->buffer_.data(), this->buffer_.data() + header.size());
		}
	};

	/// \brief Create a simple tar file as istream
	template< typename Logger, typename Fs, std::size_t buffer_size = 65536 , flag::type tar_flags = flag::truncate_growing >
	class tar_stream: public std::istream {
	public:
		explicit tar_stream(const std::set< std::string > &files):
			buf_(files, *this)
		{
			init(&buf_);
		}

	private:
		tar_buff< Logger, Fs, buffer_size, tar_flags > buf_;
	};


	/// \brief Write a simple tar file
	class tar_reader{
	public:
		tar_reader(std::string const& filename):
			isptr_(std::make_unique< std::ifstream >(
				filename.c_str(), std::ios_base::in | std::ios_base::binary
			)),
			is_(*isptr_.get())
		{
			init();
		}

		tar_reader(std::istream& is):
			is_(is)
		{
			init();
		}

		std::istream& get(std::string const& filename){
			auto iter = files_.find(filename);
			if(iter == files_.end()){
				throw std::runtime_error(
					"Filename-entry not fount in tar-file: " + filename
				);
			}

			iter->second.seekg(0);
			return iter->second;
		}


	private:
		void init(){
			static constexpr std::array< char, 512 > empty_buffer{};

			std::array< char, 512 > buffer;
			while(is_){
				is_.read(buffer.data(), 512);

				if(buffer == empty_buffer){
					is_.read(buffer.data(), 512);
					if(buffer != empty_buffer || !is_){
						throw std::runtime_error("Corrupt tar-file.");
					}
					break;
				}

				std::string filename;
				std::size_t size;
				std::tie(filename, size) =
					impl::tar::read_posix_header(buffer);

				auto result = files_.emplace(
					std::piecewise_construct,
					std::forward_as_tuple(filename),
					std::forward_as_tuple(is_.rdbuf(), is_.tellg(), size)
				);
				if(!result.second){
					throw std::runtime_error(
						"Duplicate filename-entry while reading tar-file: " +
						filename
					);
				}

				std::streampos file_size_in_tar =
					size + (512 - (size % 512)) % 512;
				is_.seekg(is_.tellg() + file_size_in_tar);


				if(!is_){
					throw std::runtime_error(
						"Tar filename-entry with illegal size: " + filename
					);
				}
			}
		}

		/// \brief Stream if read via filename
		std::unique_ptr< std::ifstream > isptr_;

		/// \brief Input stream of the tar-file
		std::istream& is_;

		/// \brief Map of filenames and contents
		std::map< std::string, io_tools::isubstream > files_;
	};


}


#endif
